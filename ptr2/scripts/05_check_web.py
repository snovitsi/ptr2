#!/usr/bin/env python3
"""Шаг 4: проверка доступности веб-сервера по хостнеймам, у которых есть A-запись.

Берёт задачи из Redis-очереди ptr2:queue:web_checks (создаёт 04_check_a.py)
или выбирает из БД хостнеймы с has_a_record = TRUE. Для каждого пробует http:// и
https:// (порт 80/443), фиксирует статус, Server-заголовок, <title>, финальный URL.
Результат — таблица web_checks; доступные серверы ставятся в очередь OWA-проверок.

Использование:
    python -m ptr2.scripts.05_check_web [--from-queue] [--from-db --limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import ssl
import sys
import time

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done  # noqa: E402
from ptr2.config import get_config, consumer, producer  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402

log = logging.getLogger("ptr2.web")

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)

UPSERT_WEB_SQL = """
INSERT INTO web_checks (hostname, url, final_url, http_status, server_header, title,
                        reachable, error)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (url) DO UPDATE
   SET final_url = EXCLUDED.final_url,
       http_status = EXCLUDED.http_status,
       server_header = EXCLUDED.server_header,
       title = EXCLUDED.title,
       reachable = EXCLUDED.reachable,
       error = EXCLUDED.error,
       checked_at = now()
RETURNING id
"""


def extract_title(body: str) -> str | None:
    m = TITLE_RE.search(body or "")
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1)).strip()[:512]


async def probe_site(session: aiohttp.ClientSession, base_url: str) -> dict:
    out = {"url": base_url, "final_url": None, "http_status": None,
           "server_header": None, "title": None, "reachable": False, "error": None}
    try:
        async with session.get(base_url, allow_redirects=True) as resp:
            body = ""
            try:
                body = await resp.text(errors="replace")
            except Exception:
                pass
            out.update(
                final_url=str(resp.url),
                http_status=resp.status,
                server_header=resp.headers.get("Server"),
                title=extract_title(body),
                reachable=resp.status < 500,
            )
    except aiohttp.ClientError as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"[:512]
    except asyncio.TimeoutError:
        out["error"] = "timeout"
    except ssl.SSLError as exc:
        out["error"] = f"ssl: {exc}"[:512]
    return out


def emit_owa_tasks(cfg, emit, hosts: set[str]) -> None:
    """Поставить доступные хосты в очередь OWA-проверок (Stream или список)."""
    if emit is None or not hosts:
        return
    emit([{"hostname": h} for h in sorted(hosts)])


async def check_hostnames(cfg, hostnames: list[str], emit_owa) -> tuple[int, int]:
    """Возвращает (проверено URL, доступно веб-серверов).

    emit_owa(list[dict]) — функция отправки задач OWA-проверки
    (Stream.send_batch или SimpleQueue.push_batch), None = не слать.
    """
    connector = aiohttp.TCPConnector(
        limit=cfg.http_concurrency, ssl=False if not cfg.verify_tls else True, ttl_dns_cache=60
    )
    timeout = aiohttp.ClientTimeout(total=cfg.http_timeout, connect=min(5.0, cfg.http_timeout))
    reachable_hosts: set[str] = set()

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers={"User-Agent": cfg.user_agent}
    ) as session:
        urls: list[tuple[str, str]] = []
        for h in hostnames:
            urls.append((h, f"https://{h}/"))
            urls.append((h, f"http://{h}/"))

        sem = asyncio.Semaphore(cfg.http_concurrency)

        async def guarded(h, u):
            async with sem:
                return h, await probe_site(session, u)

        results = await asyncio.gather(*(guarded(h, u) for h, u in urls))

    rows = []
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        for host, res in results:
            cur.execute(
                UPSERT_WEB_SQL,
                (host, res["url"], res["final_url"], res["http_status"], res["server_header"],
                 res["title"], res["reachable"], res["error"]),
            )
            if res["reachable"]:
                reachable_hosts.add(host)
        cur.close()

    emit_owa_tasks(cfg, emit_owa, reachable_hosts)

    return len(urls), len(reachable_hosts)


async def run(args) -> int:
    cfg = get_config()
    t0 = time.time()
    use_streams = cfg.streams_enabled and not args.list_queue

    if args.from_queue and use_streams:
        # распределённый воркер: Redis Stream + consumer group (много хостов)
        cons = consumer(cfg.stream_web_checks, suffix="web")
        cons.ensure_group()
        out_p = producer(cfg.stream_owa_checks)
        hosts_seen: set[str] = set()
        total = ok = 0
        idle_rounds = 0
        while True:
            msgs = cons.read(count=args.batch, block_ms=2000)
            if not msgs:
                msgs = cons.claim_stale(min_idle_ms=cfg.claim_idle_ms, count=args.batch)
            if not msgs:
                if args.once:
                    if cons.depth() == 0 and cons.pending_total() == 0:
                        break
                    idle_rounds += 1
                    if idle_rounds > 5:
                        break
                    time.sleep(1)
                continue
            idle_rounds = 0
            names = [p["hostname"] for _, p in msgs
                     if p.get("hostname") and p["hostname"] not in hosts_seen]
            hosts_seen.update(names)
            if names:
                n, o = await check_hostnames(cfg, names, out_p.send_batch)
                total += n
                ok += o
            cons.ack(*[mid for mid, _ in msgs])
            log.info("web: проверено %d URL, доступно хостов %d", total, ok)
        owa_queued = cons.r.xlen(cfg.stream_owa_checks)
        stage_done("check_web", urls_checked=total, hosts_reachable=ok,
                   owa_queued=owa_queued, mode="stream")
        print({"urls_checked": total, "hosts_reachable": ok, "owa_queued": owa_queued})
        return 0

    if args.from_queue:
        # режим воркера на старой списковой очереди
        r = get_redis(cfg.redis_url)
        q_in = SimpleQueue(r, cfg.queue_web_checks)
        q_out = SimpleQueue(r, cfg.queue_owa_checks)
        hosts_seen = set()
        total = ok = 0
        while True:
            items = []
            while len(items) < args.batch:
                it = q_in.pop(timeout=1)
                if it is None:
                    break
                items.append(it)
            if not items:
                break
            names = [p["hostname"] for _, p in items if p["hostname"] not in hosts_seen]
            hosts_seen.update(names)
            n, o = await check_hostnames(cfg, names, q_out.push_batch)
            total += n
            ok += o
            for raw, _ in items:
                q_in.done(raw)
            log.info("web: проверено %d URL, доступно хостов %d", total, ok)
        stage_done("check_web", urls_checked=total, hosts_reachable=ok,
                   owa_queued=q_out.depth(), mode="queue")
        print({"urls_checked": total, "hosts_reachable": ok, "owa_queued": q_out.depth()})
        return 0

    # из БД: хостнеймы, у которых подтверждена A-запись
    emit_owa = (producer(cfg.stream_owa_checks).send_batch
                if use_streams else SimpleQueue(get_redis(cfg.redis_url),
                                                cfg.queue_owa_checks).push_batch)
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT hostname FROM a_record_checks
             WHERE has_a_record = TRUE
             ORDER BY hostname LIMIT %s
            """,
            (args.limit,),
        )
        names = [row[0] for row in cur.fetchall()]
        cur.close()
    log.info("К проверке хостнеймов: %d", len(names))
    if names:
        total, ok = await check_hostnames(cfg, names, emit_owa)
        log.info("Готово: %d URL, %d доступных хостов (%.1f сек)", total, ok, time.time() - t0)
        stage_done("check_web", urls_checked=total, hosts_reachable=ok,
                   seconds=round(time.time() - t0, 1))
        print({"urls_checked": total, "hosts_reachable": ok})
    else:
        print({"urls_checked": 0, "hosts_reachable": 0})
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--from-queue", action="store_true", help="воркер: читать web_checks из Redis")
    src.add_argument("--from-db", action="store_true", help="выборка из a_record_checks (default)")
    ap.add_argument("--once", action="store_true",
                    help="в from-queue режиме: вычитать текущее и выйти (иначе крутится вечно)")
    ap.add_argument("--list-queue", action="store_true", help="принудительно старая списковая очередь")
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--batch", type=int, default=100)
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="check_web")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
