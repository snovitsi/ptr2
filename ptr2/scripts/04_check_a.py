#!/usr/bin/env python3
"""Шаг 3: проверка прямых A-записей для хостнеймов, найденных по PTR.

Для каждой пары (hostname, expected_ip) из таблицы ptr_records (или из
Redis-очереди a_checks, созданной 03_load_ptr.py) резолвит hostname -> A.
Результат в таблице a_record_checks: has_a_record (есть ли вообще A),
points_back (A указывает на тот же IP, с которого найден PTR).

Хостнеймы, у которых есть A-запись, автоматически ставятся в очередь
проверки веб-сервера (ptr2:queue:web_checks) — вход для скрипта 05.

Использование:
    python -m ptr2.scripts.04_check_a [--from-db] [--limit N] [--from-queue|--all]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done, worker_tag  # noqa: E402
from ptr2.config import get_config, consumer, producer  # noqa: E402
from ptr2.dnslib import DnsClient  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402
from ptr2.apayload import PAIRS_KEY, decode_pairs  # noqa: E402

log = logging.getLogger("ptr2.check_a")

UPDATE_SQL = """
UPDATE a_record_checks
   SET resolved_ips = %s::inet[],
       has_a_record = %s,
       points_back  = %s,
       checked_at   = now()
 WHERE hostname = %s AND expected_ip = %s
"""


async def check_pairs(cfg, dns: DnsClient, pairs: list[tuple[str, str]], emit) -> tuple[int, int]:
    """pairs = [(hostname, expected_ip)] — параллельная проверка с записью в PG.

    emit(list[dict]) — функция отправки задач веб-проверки (Stream или список), None = не слать.
    """
    results = []

    async def one(host: str, ip: str):
        a = await dns.query_a(host)
        results.append((host, ip, a))

    chunk = cfg.a_record_concurrency
    for i in range(0, len(pairs), chunk):
        batch = pairs[i : i + chunk]
        await asyncio.gather(*(one(h, ip) for h, ip in batch))

    rows = []
    web_tasks = []
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        for host, ip, a in results:
            has = bool(a)
            back = ip in a if has else None
            rows.append((a or None, has, back, host, ip))
            if has and emit is not None:
                web_tasks.append({"hostname": host, "ips": a})
        cur.executemany(UPDATE_SQL, rows)
        cur.close()
    if emit is not None and web_tasks:
        emit(web_tasks)
    return len(results), sum(1 for _, _, a in results if a)


async def run(args) -> int:
    cfg = get_config()
    r = get_redis(cfg.redis_url)
    q_in = SimpleQueue(r, cfg.queue_a_checks)
    q_out = SimpleQueue(r, cfg.queue_web_checks)

    dns = DnsClient(
        nameservers=cfg.dns_nameservers or None,
        timeout=cfg.dns_timeout,
        lifetime=cfg.dns_lifetime,
        concurrency=cfg.a_record_concurrency,
        rate_limit=cfg.ptr_rate_limit,
    )

    t0 = time.time()
    total = with_a = 0

    if args.from_queue and cfg.streams_enabled:
        # распределённый воркер: Redis Stream + consumer group (много хостов)
        cons = consumer(cfg.stream_a_checks, suffix="a")
        cons.ensure_group()
        out_p = producer(cfg.stream_web_checks)
        idle_rounds = 0
        last_xlen = None
        while True:
            msgs = cons.read(count=args.batch, block_ms=2000)
            if not msgs:
                msgs = cons.claim_stale(min_idle_ms=cfg.claim_idle_ms, count=args.batch)
            if not msgs:
                if args.once:
                    # depth()==lag может быть недоступен в Redis <7.2 (всегда 0),
                    # поэтому считаем длину стрима: пока она растёт, этап 03 ещё
                    # добавляет задачи — ждём; зависла и PEL пуста -> выходим.
                    xlen = cons.r.xlen(cfg.stream_a_checks)
                    if xlen == last_xlen and cons.pending_total() == 0:
                        break
                    last_xlen = xlen
                    idle_rounds += 1
                    if idle_rounds > 60:   # страховка: не более ~2 минут простоя
                        log.warning("a_checks: нет активности 60 раундов, выхожу "
                                    "(depth=%s pending=%s xlen=%s)",
                                    cons.depth(), cons.pending_total(), xlen)
                        break
                    time.sleep(1)
                    continue
                # вечный режим без --once: при пустоте просто идём дальше
                continue
            idle_rounds = 0
            pairs: list[tuple[str, str]] = []
            for _, payload in msgs:
                pairs.extend(decode_pairs(payload))
            if pairs:
                n, wa = await check_pairs(cfg, dns, pairs, out_p.send_batch)
                total += n
                with_a += wa
            cons.ack(*[mid for mid, _ in msgs])
            log.info("A-проверка: %d/%d пар имеют A-запись (всего %d)", wa, len(pairs), total)
    elif args.from_queue:
        # режим воркера на старой списковой очереди: крутимся, пока есть задачи
        while True:
            items = []
            while len(items) < args.batch:
                it = q_in.pop(timeout=1)
                if it is None:
                    break
                items.append(it)
            if not items:
                break
            pairs = [(p["hostname"], p["expected_ip"]) for _, p in items]
            n, wa = await check_pairs(cfg, dns, pairs, q_out.push_batch)
            total += n
            with_a += wa
            for raw, _ in items:
                q_in.done(raw)
            log.info("A-проверка: %d/%d пар имеют A-запись (всего %d)", wa, n, total)
    else:
        # режим разовой выборки из БД: все ptr_records без проверки или все
        with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
            cond = "" if args.all else "WHERE ac.has_a_record IS NOT TRUE OR ac.resolved_ips IS NULL"
            cur.execute(
                f"""
                SELECT DISTINCT pr.hostname, host(pr.ip)
                  FROM ptr_records pr
                  JOIN a_record_checks ac ON ac.hostname = pr.hostname
                                         AND ac.expected_ip = pr.ip
                 {cond}
                 ORDER BY pr.hostname
                 LIMIT %s
                """,
                (args.limit,),
            )
            pairs = cur.fetchall()
            cur.close()
        log.info("К проверке: %d пар hostname/IP", len(pairs))
        if pairs:
            out_emit = (producer(cfg.stream_web_checks).send_batch
                        if cfg.streams_enabled else q_out.push_batch)
            n, wa = await check_pairs(cfg, dns, pairs, out_emit)
            total, with_a = n, wa

    web_q_depth = (r.xlen(cfg.stream_web_checks)
                   if cfg.streams_enabled else q_out.depth())
    log.info(
        "Готово: проверено %d, A-запись найдена у %d (%.1f сек). web_checks: %d",
        total, with_a, time.time() - t0, web_q_depth,
    )
    stage_done("check_a", checked=total, has_a=with_a, web_queued=web_q_depth,
               seconds=round(time.time() - t0, 1))
    print({"checked": total, "has_a": with_a, "web_queued": web_q_depth})
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-queue", action="store_true",
                    help="режим воркера: читать Redis-очередь/Stream a_checks")
    ap.add_argument("--once", action="store_true",
                    help="в from-queue режиме: вычитать текущее и выйти (иначе крутится вечно)")
    ap.add_argument("--all", action="store_true", help="перепроверить даже уже проверенные")
    ap.add_argument("--limit", type=int, default=100000)
    ap.add_argument("--batch", type=int, default=200, help="пар за один проход в from-queue")
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="check_a", tag=worker_tag())
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
