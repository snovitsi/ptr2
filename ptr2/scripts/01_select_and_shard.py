#!/usr/bin/env python3
"""Шаг 1: отбор диапазонов по критериям (страна, размер) и разбиение на шарды по 256 адресов.

Диапазоны берутся из таблицы ripe_ranges (заполняется 00_fetch_ripe.py).
Критерии — из конфига (.env) или аргументов CLI:
    --countries DE,NL      список CC (или подстрока через --cc-like)
    --min-size / --max-size диапазон размеров диапазона (в адресах)
    --prefix-len 24        фильтр по длине префикса (для выровненных CIDR)
    --shard-size 256       размер шарда
    --max-ranges N         ограничить количество диапазонов (для теста)

Каждый подходящий диапазон режется на куски по shard_size адресов; каждый кусок
сохраняется в таблице shards и кладётся в Redis-очередь задач PTR-обхода.

Анти-BAN (см. ptr2/stratify.py): каждому шарду присваивается «семья» —
его /24 (FAMILY_BITS). В очередь задачи ставятся НЕ подряд по диапазонам,
а round-robin по семьям: соседние задачи обрабатываются воркерами одновременно,
поэтому в любой момент времени DNS-запросы идут к РАЗНЫМ reverse-зонам
(разным авторитетным серверам), а не плотным потоком в одну подсеть.

Распределённый режим: очередью по умолчанию служит Redis Stream
(ptr2:stream:ptr_shards) с consumer group — задачи можно разбирать
одновременно с нескольких python-хостов. USE_STREAMS=false возвращает
старую списковую очередь (BLMOVE).

Использование:
    python -m ptr2.scripts.01_select_and_shard [--dry-run] [--requeue]
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done  # noqa: E402
from ptr2.config import get_config, stream_producer  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402
from ptr2.stratify import family_of, interleave_by_family  # noqa: E402

log = logging.getLogger("ptr2.shard")

SELECT_SQL = """
SELECT id, cc, host(start_ip) AS start_ip, host(end_ip) AS end_ip, size
  FROM ripe_ranges
 WHERE type = 'ipv4'
   AND size > 0
   AND (%(countries)s::text[] IS NULL OR cc = ANY(%(countries)s))
   AND (%(cc_like)s::text IS NULL OR cc ILIKE %(cc_like)s)
   AND size >= %(min_size)s
   AND (%(max_size)s = 0 OR size <= %(max_size)s)
   AND (%(prefix_lens)s::int[] IS NULL
        OR (is_cidr_aligned AND prefix_len = ANY(%(prefix_lens)s)))
 ORDER BY size ASC, id ASC
 LIMIT %(limit)s
"""


def make_shards(start_str: str, end_str: str, shard_size: int):
    """Генерирует (shard_index, start_ip, end_ip) для диапазона."""
    a = int(ipaddress.IPv4Address(start_str))
    b = int(ipaddress.IPv4Address(end_str))
    idx = 0
    cur = a
    while cur <= b:
        seg_end = min(cur + shard_size - 1, b)
        yield idx, str(ipaddress.IPv4Address(cur)), str(ipaddress.IPv4Address(seg_end))
        cur = seg_end + 1
        idx += 1


UPSERT_SHARD_SQL = """
INSERT INTO shards (range_id, shard_index, start_ip, end_ip, family_key, status, queued_at)
VALUES (%s, %s, %s, %s, %s, 'queued', now())
ON CONFLICT (range_id, shard_index) DO UPDATE
   SET start_ip = EXCLUDED.start_ip,
       end_ip = EXCLUDED.end_ip,
       family_key = EXCLUDED.family_key,
       status = 'queued',
       queued_at = now()
RETURNING id
"""


def _push_tasks(cfg, q, sp, tasks: list[dict]) -> None:
    """Отправка задач PTR: в Redis Stream (распределённый режим) или в списковую очередь."""
    if sp is not None:
        sp.send_batch(tasks)
    elif q is not None:
        q.push_batch(tasks)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--countries", default=None, help="CC через запятую, напр. DE,NL,FR")
    ap.add_argument("--cc-like", default=None, help="SQL LIKE/ILIKE шаблон для кода страны")
    ap.add_argument("--min-size", type=int, default=None)
    ap.add_argument("--max-size", type=int, default=None)
    ap.add_argument("--prefix-len", type=int, default=None, help="точное совпадение длины префикса")
    ap.add_argument("--shard-size", type=int, default=None)
    ap.add_argument("--family-bits", type=int, default=None,
                    help="префикс «семьи» для чередования задач (по умолчанию 24 = /24)")
    ap.add_argument("--no-interleave", action="store_true",
                    help="не перемешивать задачи по семьям (старое поведение: подряд по диапазонам)")
    ap.add_argument("--max-ranges", type=int, default=None, help="лимит числа диапазонов")
    ap.add_argument("--dry-run", action="store_true", help="не писать в БД и Redis")
    ap.add_argument("--requeue", action="store_true", help="пересоздать очереди (clear)")
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="shard")

    countries = None
    raw_countries = args.countries if args.countries is not None else ",".join(cfg.target_countries)
    if raw_countries:
        countries = [c.strip().upper() for c in raw_countries.split(",") if c.strip()] or None

    min_size = args.min_size if args.min_size is not None else (cfg.min_range_size or 1)
    max_size = args.max_size if args.max_size is not None else cfg.max_range_size
    prefix_len = args.prefix_len
    if prefix_len is None and (cfg.min_prefix_len == cfg.max_prefix_len):
        prefix_len = cfg.min_prefix_len
    shard_size = args.shard_size or cfg.shard_size
    limit = args.max_ranges or 10**9

    params = {
        "countries": countries,
        "cc_like": args.cc_like,
        "min_size": min_size,
        "max_size": max_size or 0,
        "limit": limit,
    }

    # если в конфиге задан диапазон длин префиксов (min != max) — строим IN-список
    prefix_lens: list[int] | None = None
    if prefix_len is not None:
        prefix_lens = [prefix_len]
    elif cfg.min_prefix_len and cfg.max_prefix_len and (cfg.min_prefix_len, cfg.max_prefix_len) != (24, 24):
        prefix_lens = list(range(cfg.min_prefix_len, cfg.max_prefix_len + 1))
    params["prefix_lens"] = prefix_lens

    family_bits = args.family_bits or cfg.family_bits
    log.info(
        "Критерии: countries=%s cc_like=%s size=[%s..%s] prefix_len=%s shard_size=%d "
        "family_bits=/%d interleave=%s",
        countries, args.cc_like, min_size, max_size or "inf", prefix_lens or prefix_len,
        shard_size, family_bits, not args.no_interleave,
    )

    r = None if args.dry_run else get_redis(cfg.redis_url)
    q = SimpleQueue(r, cfg.queue_shards_ptr) if r is not None else None
    sp = stream_producer(cfg.stream_ptr_shards) if (r is not None and cfg.streams_enabled) else None
    if q and args.requeue:
        q.clear()
        log.info("Очередь %s очищена", q.name)
    if sp and args.requeue:
        r.delete(cfg.stream_ptr_shards)
        log.info("Stream %s удалён (пересоздан при первой отправке)", sp.stream)

    stats = {"ranges": 0, "shards": 0, "ips": 0, "families": 0}
    with db_conn(cfg.dsn()) as conn:
        cur = conn.cursor()
        cur.execute(SELECT_SQL, params)
        rows = cur.fetchall()
        log.info("Найдено подходящих диапазонов: %d", len(rows))

        all_tasks: list[dict] = []
        for range_id, cc, s, e, size in rows:
            stats["ranges"] += 1
            stats["ips"] += size
            for idx, ss, se in make_shards(s, e, shard_size):
                stats["shards"] += 1
                fam = family_of(ss, se, family_bits)
                if args.dry_run:
                    all_tasks.append({"range_id": range_id, "shard_index": idx,
                                      "cc": cc, "start": ss, "end": se,
                                      "family_key": fam})
                    continue
                cur.execute(UPSERT_SHARD_SQL, (range_id, idx, ss, se, fam))
                shard_id = cur.fetchone()[0]
                all_tasks.append(
                    {"shard_id": shard_id, "range_id": range_id, "shard_index": idx,
                     "cc": cc, "start": ss, "end": se, "family_key": fam}
                )
        stats["families"] = len({t["family_key"] for t in all_tasks})

        # Анти-BAN: чередуем семьи в порядке раздачи задач.
        if not args.no_interleave:
            all_tasks = interleave_by_family(all_tasks)

        if not args.dry_run:
            for i in range(0, len(all_tasks), 1000):
                _push_tasks(cfg, q, sp, all_tasks[i : i + 1000])
            cur.execute(
                "UPDATE pipeline_runs SET stats = "
                "jsonb_set(jsonb_set(COALESCE(stats,'{}'::jsonb),'{select_ranges}', to_jsonb(%s::int)),"
                "'{families}', to_jsonb(%s::int)),"
                " finished_at = now() WHERE stage='select_and_shard'",
                (stats["ranges"], stats["families"]),
            )
        else:
            head = [t["family_key"] for t in all_tasks[:24]]
            log.info("DRY-RUN: первые 24 семьи в порядке раздачи: %s", head)
        cur.close()

    if not args.dry_run:
        if sp is not None:
            log.info("В stream %s сообщений: %d", sp.stream, r.xlen(sp.stream))
        elif q is not None:
            log.info("В очереди PTR-шардов задач: %d", q.depth())
    log.info("Итог: %s", stats)
    stage_done("shard", ranges=stats["ranges"], shards=stats["shards"],
               ips=stats["ips"], families=stats["families"], dry_run=args.dry_run)
    print(json.dumps(stats, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
