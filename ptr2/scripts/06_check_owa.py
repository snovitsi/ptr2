#!/usr/bin/env python3
"""Шаг 5: поиск Outlook Web Access (Exchange OWA/ECP) за доступными веб-серверами.

Для каждого хоста из очереди ptr2:stream:owa_checks (или ptr2:queue:owa_checks
в legacy-режиме, либо из web_checks.reachable=TRUE при --from-db):
 1) пробует пути /owa/, /owa/auth/logon.aspx, /autodiscover/autodiscover.xml;
 2) анализирует редиректы базовых страниц (частый признак Exchange:
    / -> /owa или /owa/auth/logon.aspx);
 3) ищет в HTML-ответах OWA-маркеры (OWA version, "Outlook Web App",
    "Microsoft.Exchange.Clients.Owa", x-owa-version, feOwaVersion и т.п.).

Результат — таблица owa_checks (is_owa, confidence, evidence).
В режиме потоков (по умолчанию) работает как распределённый воркер: Redis Stream
+ consumer group, несколько экземпляров параллельно; --once завершает воркер,
когда очередь пуста и не растёт (аналогично этапам 04/05).

Использование:
    python -m ptr2.scripts.06_check_owa [--from-queue [--once]] [--list-queue]
                                        [--from-db --limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import time

import aiohttp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done, worker_tag  # noqa: E402
from ptr2.config import get_config  # noqa: E402
from ptr2.owa_version import extract_versions, resolve_version  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402
from ptr2.config import consumer  # noqa: E402

log = logging.getLogger("ptr2.owa")

# маркеры OWA/Exchange в содержимом ответов (совпадение по началу строки evidence)
BODY_MARKERS = [
    ("outlook web app", "high"),
    ("outlook on the web", "high"),
    ("microsoft.exchange.clients.owa", "high"),
    ("owa version", "high"),
    ("feowaversion", "high"),
    ("msapplication-notificationurl", "medium"),
    ("/owa/auth/logon.aspx", "medium"),
    ("exchangecookie", "medium"),
    ("x-owa-version", "medium"),
    ("cookie x-owa", "low"),
]
HEADER_MARKERS = {
    "x-owa-version": ("header: x-owa-version", "high"),
    "x-exchange-cookie": ("header: x-exchange-cookie", "medium"),
    "x-feserver": ("header: x-feserver", "medium"),
    "x-backendserver": ("header: x-backendserver", "low"),
}

UPSERT_OWA_SQL = """
INSERT INTO owa_checks (hostname, url, probe_url, is_owa, confidence, evidence,
                        http_status, redirect_chain,
                        exchange_version, build_number, version_source)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (hostname, url, probe_url) DO UPDATE
   SET is_owa = EXCLUDED.is_owa,
       confidence = EXCLUDED.confidence,
       evidence = EXCLUDED.evidence,
       http_status = EXCLUDED.http_status,
       redirect_chain = EXCLUDED.redirect_chain,
       exchange_version = COALESCE(EXCLUDED.exchange_version, owa_checks.exchange_version),
       build_number = COALESCE(EXCLUDED.build_number, owa_checks.build_number),
       version_source = COALESCE(EXCLUDED.version_source, owa_checks.version_source),
       checked_at = now()
"""


def detect_markers(resp_headers, body_snippet: str, final_url: str, base_url: str):
    """Возвращает список (evidence, confidence)."""
    ev: list[tuple[str, str]] = []
    hl = {k.lower(): v for k, v in resp_headers.items()}
    for h, (label, conf) in HEADER_MARKERS.items():
        if h in hl:
            ev.append((f"{label}={hl[h][:64]}", conf))
    low_body = (body_snippet or "").lower()
    for marker, conf in BODY_MARKERS:
        if marker in low_body:
            ev.append((f"body contains '{marker}'", conf))
    fu = (final_url or "").lower()
    if "/owa" in fu:
        ev.append((f"redirect/final URL contains /owa ({final_url[:120]})", "high"))
    return ev


CONF_RANK = {"high": 3, "medium": 2, "low": 1}


async def probe(session, url: str) -> dict | None:
    try:
        async with session.get(url, allow_redirects=True) as resp:
            body = ""
            try:
                body = await resp.text(errors="replace")
            except Exception:
                pass
            chain = [str(h.url) for h in resp.history] + [str(resp.url)]
            cookies = {c.key: c.value for c in session.cookie_jar}
            return {
                "status": resp.status,
                "headers": dict(resp.headers),
                "final_url": str(resp.url),
                "chain": chain,
                "snippet": body[:64 * 1024],
                "cookies": cookies,
            }
    except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
        return None


def best_conf(ev: list[tuple[str, str]]) -> str | None:
    if not ev:
        return None
    return max((c for _, c in ev), key=lambda c: CONF_RANK[c])


async def check_host(cfg, session, host: str) -> list[dict]:
    """Проверяет один хост по всем схемам/путям; возвращает строки для owa_checks.

    Версия Exchange определяется по совокупности всех успешных ответов
    (заголовок X-Owa-Version, feOwaVersion в теле, meta msexccver, cookie ClientInfo).
    """
    rows: list[dict] = []
    bases = [f"https://{host}/", f"http://{host}/"]
    any_reachable = False
    all_candidates: list[dict] = []

    for base in bases:
        # 1) базовая страница — смотрим редирект на /owa
        r = await probe(session, base)
        if r:
            any_reachable = True
            all_candidates.extend(extract_versions(r["headers"], r["snippet"], r["cookies"]))
            ev = detect_markers(r["headers"], r["snippet"], r["final_url"], base)
            if ev:
                rows.append({
                    "hostname": host, "url": base, "probe_url": base,
                    "is_owa": True,
                    "confidence": best_conf(ev),
                    "evidence": "; ".join(e for e, _ in ev)[:1024],
                    "http_status": r["status"],
                    "redirect_chain": " -> ".join(r["chain"])[:1024],
                })
        # 2) прямые пути OWA
        for path in cfg.owa_paths:
            u = base.rstrip("/") + "/" + path.lstrip("/")
            r2 = await probe(session, u)
            if not r2:
                continue
            all_candidates.extend(extract_versions(r2["headers"], r2["snippet"], r2["cookies"]))
            ev2 = detect_markers(r2["headers"], r2["snippet"], r2["final_url"], u)
            looks_owa = bool(ev2) or (r2["status"] in (200, 401, 440) and "/owa" in u.lower())
            rows.append({
                "hostname": host, "url": base, "probe_url": u,
                "is_owa": bool(looks_owa),
                "confidence": best_conf(ev2) or ("low" if looks_owa else None),
                "evidence": "; ".join(e for e, _ in ev2)[:1024] or None,
                "http_status": r2["status"],
                "redirect_chain": " -> ".join(r2["chain"])[:1024],
            })

    ver, build, vsrc = resolve_version(all_candidates)
    owa_rows = [row for row in rows if row["is_owa"]]
    for row in rows:
        row["exchange_version"] = ver if owa_rows else None
        row["build_number"] = build if owa_rows else None
        row["version_source"] = vsrc if owa_rows else None

    if not any_reachable and not rows:
        rows.append({
            "hostname": host, "url": f"https://{host}/", "probe_url": None,
            "is_owa": False, "confidence": None,
            "evidence": "no HTTP response on 80/443", "http_status": None,
            "redirect_chain": None,
            "exchange_version": None, "build_number": None, "version_source": None,
        })
    return rows


async def run(args) -> int:
    cfg = get_config()
    t0 = time.time()

    connector = aiohttp.TCPConnector(limit=cfg.http_concurrency, ssl=False)
    timeout = aiohttp.ClientTimeout(total=cfg.http_timeout, connect=min(5.0, cfg.http_timeout))

    async def process(hosts: list[str], session) -> tuple[int, int]:
        found = 0
        sem = asyncio.Semaphore(cfg.http_concurrency)

        async def guarded(h):
            async with sem:
                return h, await check_host(cfg, session, h)

        results = await asyncio.gather(*(guarded(h) for h in hosts))
        rows_written = 0
        with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
            for host, rows in results:
                for row in rows:
                    # в owa_checks пишем только строки с определённой версией OWA/Exchange
                    if not row.get("exchange_version"):
                        continue
                    cur.execute(
                        UPSERT_OWA_SQL,
                        (row["hostname"], row["url"], row["probe_url"], row["is_owa"],
                         row["confidence"], row["evidence"], row["http_status"],
                         row["redirect_chain"], row.get("exchange_version"),
                         row.get("build_number"), row.get("version_source")),
                    )
                    rows_written += 1
                    if row["is_owa"]:
                        found += 1
            cur.close()
        return len(hosts), found

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers={"User-Agent": cfg.user_agent}
    ) as session:
        use_streams = cfg.streams_enabled and not args.list_queue

        if args.from_queue and use_streams:
            # распределённый воркер: Redis Stream + consumer group (несколько экземпляров)
            cons = consumer(cfg.stream_owa_checks, suffix="owa")
            cons.ensure_group()
            total = hits = 0
            idle_rounds = 0
            last_xlen = None
            while True:
                msgs = cons.read(count=args.batch, block_ms=2000)
                if not msgs:
                    msgs = cons.claim_stale(min_idle_ms=cfg.claim_idle_ms, count=args.batch)
                if not msgs:
                    if args.once:
                        # depth()==lag недоступен в Redis <7.2 -> контролируем рост XLEN:
                        # пока этап 05 добавляет задачи, ждём; зависла и PEL пуста -> выход.
                        xlen = cons.r.xlen(cfg.stream_owa_checks)
                        if xlen == last_xlen and cons.pending_total() == 0:
                            break
                        last_xlen = xlen
                        idle_rounds += 1
                        if idle_rounds > 60:   # страховка: не более ~2 минут простоя
                            log.warning("owa_checks: нет активности 60 раундов, выхожу "
                                        "(depth=%s pending=%s xlen=%s)",
                                        cons.depth(), cons.pending_total(), xlen)
                            break
                        time.sleep(1)
                        continue
                    continue
                idle_rounds = 0
                hosts = sorted({p["hostname"] for _, p in msgs if p.get("hostname")})
                if hosts:
                    n, f = await process(hosts, session)
                    total += n
                    hits += f
                cons.ack(*[mid for mid, _ in msgs])
                log.info("OWA: хостов %d, OWA-подтверждений %d", total, hits)
            stage_done("owa", hosts_checked=total, owa_rows=hits, mode="stream")
            print({"hosts_checked": total, "owa_rows": hits})
            return 0

        if args.from_queue:
            r = get_redis(cfg.redis_url)
            q_in = SimpleQueue(r, cfg.queue_owa_checks)
            # зависшие в processing задачи (например, после Ctrl+C воркера) — обратно в очередь
            reclaimed = q_in.reclaim_stale(max_idle_sec=cfg.claim_idle_ms // 1000)
            if reclaimed:
                log.info("OWA: возвращено в очередь %d зависших задач", reclaimed)
            total = owa_hits = 0
            while True:
                items = []
                while len(items) < args.batch:
                    it = q_in.pop(timeout=1)
                    if it is None:
                        break
                    items.append(it)
                if not items:
                    break
                hosts = sorted({p["hostname"] for _, p in items})
                n, f = await process(hosts, session)
                total += n
                owa_hits += f
                for raw, _ in items:
                    q_in.done(raw)
                log.info("OWA: хостов %d, срабатываний %d", total, owa_hits)
            stage_done("owa", hosts_checked=total, owa_rows=owa_hits, mode="queue")
            print({"hosts_checked": total, "owa_rows": owa_hits})
            return 0

        with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT hostname FROM web_checks
                 WHERE reachable = TRUE
                   AND hostname NOT IN (SELECT hostname FROM owa_checks)
                 ORDER BY hostname LIMIT %s
                """,
                (args.limit,),
            )
            hosts = [row[0] for row in cur.fetchall()]
            cur.close()
        log.info("К проверке OWA хостов: %d", len(hosts))
        if hosts:
            total, hits = await process(hosts, session)
            log.info("Готово: %d хостов, %d OWA-подтверждений (%.1f сек)",
                     total, hits, time.time() - t0)
            stage_done("owa", hosts_checked=total, owa_rows=hits,
                       seconds=round(time.time() - t0, 1))
            print({"hosts_checked": total, "owa_rows": hits})
        else:
            print({"hosts_checked": 0, "owa_rows": 0})
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--from-queue", action="store_true",
                     help="воркер: читать owa_checks из Redis (stream по умолчанию)")
    src.add_argument("--from-db", action="store_true", help="выборка из web_checks (default)")
    ap.add_argument("--list-queue", action="store_true",
                    help="использовать legacy-список вместо Streams")
    ap.add_argument("--once", action="store_true",
                    help="(streams) выйти, когда очередь пуста и не растёт")
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--batch", type=int, default=50)
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="owa", tag=worker_tag())
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
