#!/usr/bin/env python3
"""Шаг 2b: вычитывание результатов PTR из Redis и запись в PostgreSQL.

Читает stream ptr2:stream:ptr_results (или старую очередь ptr_results при
USE_STREAMS=false) пачками, UPSERT'ит в ptr_records. Каждой уникальной паре
(hostname, expected_ip) создаётся строка в a_record_checks и задача в stream
ptr2:stream:a_checks — вход для скрипта 04 (его можно запускать на другом
python-хосте).

Рекомендуется держать ОДИН процесс загрузчика (он не DNS-bottleneck), но
при необходимости можно запустить несколько — они попадут в одну consumer
group и разберут сообщения без дублей записи (UPSERT идемпотентен).

Использование:
    python -m ptr2.scripts.03_load_ptr [--once] [--batch 1000]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done  # noqa: E402
from ptr2.config import get_config, consumer, producer  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402
from ptr2.apayload import encode_pairs  # noqa: E402

log = logging.getLogger("ptr2.load_ptr")

UPSERT_PTR_SQL = """
INSERT INTO ptr_records (ip, hostname, shard_id)
VALUES (%s, %s, %s)
ON CONFLICT (ip, hostname) DO NOTHING
"""

VALID_SHARD_SQL = """
SELECT EXISTS (SELECT 1 FROM shards WHERE id = %s)
"""

UPSERT_A_TASK_SQL = """
INSERT INTO a_record_checks (hostname, expected_ip)
VALUES (%s, %s)
ON CONFLICT (hostname, expected_ip) DO NOTHING
RETURNING id
"""


def write_batch(cfg, items: list[dict], q_a, qa_stream) -> int:
    """Один транзакционный батч в PG; возвращает число новых пар (hostname, ip)."""
    new_hosts: set[tuple[str, str]] = set()
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        # защита от «мусорных» shard_id из очереди (например, после пересоздания шардов)
        known: dict[int | None, bool] = {}
        rows = []
        for p in items:
            sid = p.get("shard_id")
            if sid is not None:
                sid = int(sid)
                if sid not in known:
                    cur.execute(VALID_SHARD_SQL, (sid,))
                    known[sid] = bool(cur.fetchone()[0])
            rows.append((p["ip"], p["hostname"], sid if known.get(sid) else None))
        cur.executemany(UPSERT_PTR_SQL, rows)
        pairs = {(p["ip"], p["hostname"]) for p in items}
        for ip, host in pairs:
            cur.execute(
                "SELECT 1 FROM a_record_checks WHERE hostname=%s AND expected_ip=%s",
                (host, ip),
            )
            if cur.fetchone() is None:
                new_hosts.add((host, ip))
        if new_hosts:
            cur.executemany(UPSERT_A_TASK_SQL, sorted(new_hosts))
        cur.close()
    if new_hosts:
        tasks = [{"hostname": h, "expected_ip": ip} for h, ip in sorted(new_hosts)]
        if qa_stream is not None:
            # группируем пары в одно сообщение — меньше сообщений в stream
            qa_stream.send_batch([encode_pairs(sorted(new_hosts))])
        elif q_a is not None:
            q_a.push_batch(tasks)
    return len(new_hosts)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true",
                    help="выйти, когда ptr_results опустеет (XLEN зависла + PEL чиста). "
                         "ВНИМАНИЕ: в legacy-режиме (USE_STREAMS=false) --once выходит "
                         "немедленно при первой пустоте очереди — в пайплайне там его не используют")
    ap.add_argument("--batch", type=int, default=None)
    ap.add_argument("--list-queue", action="store_true", help="принудительно старая списковая очередь")
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="load_ptr")
    batch = args.batch or cfg.db_batch_size

    r = get_redis(cfg.redis_url)
    use_streams = cfg.streams_enabled and not args.list_queue

    total = new_pairs = 0
    t0 = time.time()

    if use_streams:
        cons = consumer(cfg.stream_ptr_results, suffix="loader")
        cons.ensure_group()
        qa_stream = producer(cfg.stream_a_checks)
        idle_rounds = 0
        last_xlen = None
        while True:
            msgs = cons.read(count=batch, block_ms=2000)
            if not msgs:
                # нет новых — попробуем перехватить зависшие (после падения других загрузчиков)
                msgs = cons.claim_stale(min_idle_ms=cfg.claim_idle_ms, count=batch)
            if not msgs:
                if args.once:
                    # depth()==lag недоступен в Redis <7.2 (всегда 0): пока PTR-воркеры
                    # ещё пишут ptr_results (XLEN растёт), ждём; зависла и PEL пуста -> выход.
                    xlen = cons.r.xlen(cfg.stream_ptr_results)
                    if xlen == last_xlen and cons.pending_total() == 0:
                        break
                    last_xlen = xlen
                    idle_rounds += 1
                    if idle_rounds > 60:   # страховка: не более ~2 минут простоя
                        log.warning("ptr_results: нет активности 60 раундов, выхожу "
                                    "(depth=%s pending=%s xlen=%s)",
                                    cons.depth(), cons.pending_total(), xlen)
                        break
                    time.sleep(1)
                    continue
                # вечный режим без --once: при пустоте идём дальше
                continue
            idle_rounds = 0
            items = [payload for _, payload in msgs]
            n_new = write_batch(cfg, items, None, qa_stream)
            cons.ack(*[mid for mid, _ in msgs])
            total += len(items)
            new_pairs += n_new
            log.info("записано %d записей (новых пар %d, всего %d, %.1f сек)",
                     len(items), new_pairs, total, time.time() - t0)
        log.info("Готово: %d записей. Stream a_checks сообщений: %d",
                 total, r.xlen(cfg.stream_a_checks))
        stage_done("load_ptr", loaded=total, new_pairs=new_pairs,
                   a_checks_queued=r.xlen(cfg.stream_a_checks))
        print({"loaded": total, "a_checks_queued": r.xlen(cfg.stream_a_checks)})
        return 0

    # ------- legacy списковая очередь -------
    q_res = SimpleQueue(r, cfg.queue_ptr_results)
    q_a = SimpleQueue(r, cfg.queue_a_checks)
    while True:
        items: list[tuple[str, dict]] = []
        while len(items) < batch:
            it = q_res.pop(timeout=1)
            if it is None:
                break
            items.append(it)
        if not items:
            if args.once:
                break
            time.sleep(2)
            continue
        n_new = write_batch(cfg, [p for _, p in items], q_a, None)
        for raw, _ in items:
            q_res.done(raw)
        total += len(items)
        new_pairs += n_new
        log.info("записано %d записей (всего %d, %.1f сек)", total, total, time.time() - t0)
    log.info("Готово: %d записей. Очередь ptr_results: %d, a_checks: %d",
             total, q_res.depth(), q_a.depth())
    stage_done("load_ptr", loaded=total, new_pairs=new_pairs, a_checks_queued=q_a.depth())
    print({"loaded": total, "a_checks_queued": q_a.depth()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
