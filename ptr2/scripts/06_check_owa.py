#!/usr/bin/env python3
"""Шаг 5: поиск Outlook Web Access (Exchange OWA/ECP) за доступными веб-серверами.

Для каждого хоста из очереди ptr2:queue:owa_checks (или из web_checks.reachable=TRUE):
 1) пробует пути /owa/, /owa/auth/logon.aspx, /autodiscover/autodiscover.xml;
 2) анализирует редиректы базовых страниц (частый признак Exchange:
    / -> /owa или /owa/auth/logon.aspx);
 3) ищет в HTML-ответах OWA-маркеры (OWA version, "Outlook Web App",
    "Microsoft.Exchange.Clients.Owa", x-owa-version, feOwaVersion и т.п.).

Результат — таблица owa_checks (is_owa, confidence, evidence).
В таблицу пишутся только строки с подтверждённым OWA И определённой версией
Exchange (exchange_version IS NOT NULL) — «пустые» отрицательные проверки
в БД не накапливаются.

Использование:
    python -m ptr2.scripts.06_check_owa [--from-queue] [--from-db --limit N]
    python -m ptr2.scripts.06_check_owa --from-queue --once   # для pipeline
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
from ptr2.logsetup import setup_logging, stage_done  # noqa: E402
from ptr2.config import get_config  # noqa: E402
from ptr2.owa_version import extract_versions, resolve_version  # noqa: E402
from ptr2.redisq import SimpleQueue, consumer, get_redis, producer  # noqa: E402

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
        # сервер не ответил — в owa_checks ничего не пишем (см. шапку модуля)
        return []
    return rows


def _store(conn, results: list[list[dict]]) -> int:
    """Пишет в owa_checks только OWA-подтверждённые строки с определённой версией."""
    found = 0
    with conn.cursor() as cur:
        for rows in results:
            for row in rows:
                if not row["is_owa"] or not row.get("exchange_version"):
                    continue
                cur.execute(
                    UPSERT_OWA_SQL,
                    (row["hostname"], row["url"], row["probe_url"], row["is_owa"],
                     row["confidence"], row["evidence"], row["http_status"],
                     row["redirect_chain"], row.get("exchange_version"),
                     row.get("build_number"), row.get("version_source")),
                )
                found += 1
    return found


async def run(args) -> int:
    cfg = get_config()
    t0 = time.time()

    connector = aiohttp.TCPConnector(limit=cfg.http_concurrency, ssl=False)
    timeout = aiohttp.ClientTimeout(total=cfg.http_timeout, connect=min(5.0, cfg.http_timeout))

    async def process(hosts: list[str], session) -> tuple[int, int]:
        sem = asyncio.Semaphore(cfg.http_concurrency)

        async def guarded(h):
            async with sem:
                return h, await check_host(cfg, session, h)

        results = await asyncio.gather(*(guarded(h) for h in hosts))
        with db_conn(cfg.dsn()) as conn:
            found = _store(conn, [rows for _, rows in results])
        return len(hosts), found

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers={"User-Agent": cfg.user_agent}
    ) as session:
        if args.from_queue and cfg.streams_enabled:
            # распределённый воркер: Redis Stream + consumer group (как этапы 04/05);
            # завершение в --once — по пустому стриму И опустошённой PEL,
            # без зависимости от XINFO lag (недоступен на Redis < 7.2)
            cons = consumer(cfg.stream_owa_checks, suffix="owa")
            cons.ensure_group()
            total = owa_hits = 0
            idle_rounds = 0
            while True:
                msgs = cons.read(count=args.batch, block_ms=2000)
                if not msgs:
                    msgs = cons.claim_stale(min_idle_ms=cfg.claim_idle_ms,
                                            count=args.batch)
                if not msgs:
                    if args.once:
                        if cons.depth() == 0 and cons.pending_total() == 0:
                            break
                        idle_rounds += 1
                        if idle_rounds > 5:
                            log.info("OWA: стрим/PEL не очищаются за 5 раундов, выходим")
                            break
                        time.sleep(1)
                    continue
                idle_rounds = 0
                hosts = sorted({p["hostname"] for _, p in msgs
                                if isinstance(p, dict) and p.get("hostname")})
                if hosts:
                    n, f = await process(hosts, session)
                    total += n
                    owa_hits += f
                cons.ack(*[mid for mid, _ in msgs])
                log.info("OWA: хостов %d, срабатываний %d", total, owa_hits)
            stage_done("owa", hosts_checked=total, owa_rows=owa_hits, mode="stream")
            print({"hosts_checked": total, "owa_rows": owa_hits})
            return 0

        if args.from_queue:
            r = get_redis(cfg.redis_url)
            q_in = SimpleQueue(r, cfg.queue_owa_checks)
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
    src.add_argument("--from-queue", action="store_true", help="воркер: читать owa_checks из Redis")
    src.add_argument("--from-db", action="store_true", help="выборка из web_checks (default)")
    ap.add_argument("--once", action="store_true",
                    help="в режиме --from-queue: вычитать текущее и выйти (для run_pipeline.sh)")
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--batch", type=int, default=50)
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="owa")
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
