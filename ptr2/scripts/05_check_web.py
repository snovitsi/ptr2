#!/usr/bin/env python3
"""Шаг 4: проверка доступности веб-сервера по хостнеймам, у которых есть A-запись.

Берёт задачи из Redis-очереди ptr2:queue:web_checks (создаёт 04_check_a.py)
или выбирает из БД хостнеймы с has_a_record = TRUE. Для каждого пробует http:// и
https:// (порт 80/443), фиксирует статус, Server-заголовок, <title>, финальный URL.
В таблицу web_checks пишутся ТОЛЬКО ответы веб-сервера (получен HTTP-статус);
без ответа (таймаут, отказ соединения, DNS/TLS-ошибка) запись не создаётся.
Доступные серверы ставятся в очередь OWA-проверок.

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
from ptr2.logsetup import setup_logging, stage_done, worker_tag  # noqa: E402
from ptr2.config import get_config, consumer, producer  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402

log = logging.getLogger("ptr2.web")

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)

# Пишутся только URL, по которым веб-сервер ответил (http_status IS NOT NULL).
# Ошибочные строки (reachable=FALSE, http_status=NULL) из прошлых прогонов
# удаляются, если сервер снова не ответил.
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
WHERE web_checks.http_status IS NOT NULL OR EXCLUDED.http_status IS NOT NULL
RETURNING id
"""

DELETE_NOANSWER_SQL = """
DELETE FROM web_checks
 WHERE hostname = ANY(%s)
   AND http_status IS NULL
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


def _answered(res: dict) -> bool:
    """Веб-сервер ответил (есть HTTP-статус) — такую проверку пишем в БД.

    Без ответа (таймаут/отказ соединения/DNS/TLS) в web_checks ничего не
    сохраняется; если сервер ранее отвечал, а теперь молчит, старая запись
    с http_status остаётся нетронутой (не затирается ошибкой).
    """
    return res.get("http_status") is not None


async def check_hostnames(cfg, hostnames: list[str], emit_owa) -> tuple[int, int]:
    """Возвращает (записано ответов в web_checks, доступно веб-серверов).

    В БД попадают только URL, по которым сервер ответил; без ответа — не пишем.
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

    rows_written = 0
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        # в web_checks пишем только ответы веб-сервера (http_status получен);
        # без ответа запись не создаётся и не обновляется
        for host, res in results:
            if not _answered(res):
                continue
            cur.execute(
                UPSERT_WEB_SQL,
                (host, res["url"], res["final_url"], res["http_status"], res["server_header"],
                 res["title"], res["reachable"], res["error"]),
            )
            rows_written += 1
            if res["reachable"]:
                reachable_hosts.add(host)
        # чистим «ошибочные» строки (http_status IS NULL) из старых версий кода
        cur.execute(DELETE_NOANSWER_SQL, (list(set(hostnames)),))
        deleted = cur.rowcount
        if deleted:
            log.info("web: удалено %d записей без ответа от прошлых прогонов", deleted)
        cur.close()

    emit_owa_tasks(cfg, emit_owa, reachable_hosts)

    return rows_written, len(reachable_hosts)


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
        last_xlen = None
        while True:
            msgs = cons.read(count=args.batch, block_ms=2000)
            if not msgs:
                msgs = cons.claim_stale(min_idle_ms=cfg.claim_idle_ms, count=args.batch)
            if not msgs:
                if args.once:
                    # depth()==lag недоступен в Redis <7.2 -> контролируем рост XLEN:
                    # пока этап 04 добавляет задачи, ждём; зависла и PEL пуста -> выход.
                    xlen = cons.r.xlen(cfg.stream_web_checks)
                    last_xlen = xlen
                    idle_rounds += 1
                    if idle_rounds > 60:   # страховка: не более ~2 минут простоя
                        log.warning("web_checks: нет активности 60 раундов, выхожу "
                                    "(depth=%s pending=%s xlen=%s)",
                                    cons.depth(), cons.pending_total(), xlen)
                        break
                    time.sleep(1)
                    continue
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
            log.info("web: записано ответов %d, доступно хостов %d", total, ok)
        owa_queued = cons.r.xlen(cfg.stream_owa_checks)
        stage_done("check_web", rows_written=total, hosts_reachable=ok,
                   owa_queued=owa_queued, mode="stream")
        print({"rows_written": total, "hosts_reachable": ok, "owa_queued": owa_queued})
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
            log.info("web: записано ответов %d, доступно хостов %d", total, ok)
        stage_done("check_web", rows_written=total, hosts_reachable=ok,
                   owa_queued=q_out.depth(), mode="queue")
        print({"rows_written": total, "hosts_reachable": ok, "owa_queued": q_out.depth()})
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
        log.info("Готово: записано %d ответов, %d доступных хостов (%.1f сек)", total, ok, time.time() - t0)
        stage_done("check_web", rows_written=total, hosts_reachable=ok,
                   seconds=round(time.time() - t0, 1))
        print({"rows_written": total, "hosts_reachable": ok})
    else:
        print({"rows_written": 0, "hosts_reachable": 0})
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
    setup_logging(cfg.log_level, name="check_web", tag=worker_tag())
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
