#!/usr/bin/env python3
"""Шаг 0: скачивание delegated-ripencc-latest с ftp.ripe.net и загрузка в PostgreSQL.

Использование:
    python -m ptr2.scripts.00_fetch_ripe [--url URL] [--file FILE] [--limit N]
                                         [--force-download]

Экономия трафика: файл RIPE обновляется ~1 раз в сутки, поэтому скачивание
выполняется только если локальной копии нет, ей больше RIPE_CACHE_TTL_HOURS
(по умолчанию 24 ч) или изменился SHA-256 содержимого (HEAD+GET проверка).
Можно принудительно скачать заново: --force-download / FORCE_DOWNLOAD=true.

Скрипт идемпотентен: строки UPSERT'ятся по (start_ip, end_ip, type).
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import ipaddress
import logging
import os
import sys
import time
import urllib.error
import urllib.request

# добавляем корень проекта, если запускаем файл напрямую
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import cidr_aligned, db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done, stage_fail  # noqa: E402
from ptr2.config import get_config  # noqa: E402
from ptr2.db_schema import init_schema  # noqa: E402

log = logging.getLogger("ptr2.fetch")

UPSERT_SQL = """
INSERT INTO ripe_ranges (registry, cc, type, start_ip, end_ip, prefix_len, size,
                         is_cidr_aligned, source_file)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_ip, end_ip, type) DO UPDATE
   SET cc = EXCLUDED.cc,
       registry = EXCLUDED.registry,
       prefix_len = EXCLUDED.prefix_len,
       size = EXCLUDED.size,
       is_cidr_aligned = EXCLUDED.is_cidr_aligned,
       source_file = EXCLUDED.source_file,
       fetched_at = now()
"""


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(url: str, dest: str, force: bool = False, ttl_hours: float = 24.0) -> str:
    """Скачать RIPE-файл только если он реально нужен (экономия трафика).

    Пропускаем скачивание, когда локальная копия свежая (moldestage < ttl)
    И удалённое содержимое не изменилось по SHA-256. Быстрая проверка через
    HEAD (Content-Length / Last-Modified / ETag); если HEAD неприменим —
    GET с сравнением хеша. Любая сетевая ошибка при проверке -> скачиваем.
    """
    reason = None
    if force:
        reason = "--force-download"
    elif not os.path.exists(dest):
        reason = "локальной копии нет"
    else:
        age_h = (time.time() - os.path.getmtime(dest)) / 3600.0
        if age_h >= ttl_hours:
            reason = f"копии {age_h:.1f} ч (> TTL {ttl_hours:g} ч)"

    if reason is None:
        local_size = os.path.getsize(dest)
        try:  # быстрая HEAD-проверка: размер/ETag/Last-Modified
            head = urllib.request.Request(url, method="HEAD",
                                          headers={"User-Agent": "ptr2/1.0"})
            with urllib.request.urlopen(head, timeout=30) as resp:
                clen = resp.headers.get("Content-Length")
                remote_etag = resp.headers.get("ETag")
                remote_lm = resp.headers.get("Last-Modified")
            if clen and int(clen) != local_size:
                reason = f"размер изменился ({local_size} -> {clen})"
            else:
                # размер тот же или неизвестен — сверяем контрольную сумму GET'ом
                log.info("Проверяю актуальность RIPE-файла (HEAD ok, сверяю SHA-256)…")
                get = urllib.request.Request(url, headers={"User-Agent": "ptr2/1.0"})
                with urllib.request.urlopen(get, timeout=120) as resp:
                    data = resp.read()
                if data[:2] == b"\x1f\x8b":
                    data = gzip.decompress(data)
                if _sha256_bytes(data) != _sha256_file(dest):
                    reason = "содержимое изменилось (SHA-256)"
                else:
                    log.info("RIPE-файл не менялся (TTL %.0f ч, SHA-256 совпадает) — "
                             "скачивание пропущено: %s", ttl_hours, dest)
                    return dest
        except Exception as exc:  # noqa: BLE001 — сеть недоступна: консервативно используем локальный файл
            log.warning("Не удалось проверить актуальность (%s) — использую локальную копию %s",
                        exc, dest)
            return dest

    log.info("Скачиваю %s -> %s (%s)", url, dest, reason)
    req = urllib.request.Request(url, headers={"User-Agent": "ptr2/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = resp.read()
    if data[:2] == b"\x1f\x8b":  # gzip
        data = gzip.decompress(data)
    tmp = dest + ".part"
    with open(tmp, "wb") as fh:
        fh.write(data)
    os.replace(tmp, dest)
    log.info("Скачано %d байт (SHA-256 %s…)", len(data), _sha256_bytes(data)[:16])
    return dest


def iter_records(path: str):
    """Построчный парсинг delegated-файла. Пропускает комментарии/# и записи asn/eof."""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) < 5:
                continue
            registry, cc, rtype, start_s, count_s = parts[0], parts[1], parts[2], parts[3], parts[4]
            if rtype not in ("ipv4", "ipv6"):
                continue  # asn/eof пропускаем
            try:
                count = int(count_s)
            except ValueError:
                continue
            yield registry, cc.upper(), rtype, start_s, count


def load_into_db(cfg, path: str, limit: int | None = None) -> dict:
    stats = {"parsed": 0, "inserted": 0, "skipped": 0}
    batch: list[tuple] = []
    t0 = time.time()
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        for registry, cc, rtype, start_s, count in iter_records(path):
            stats["parsed"] += 1
            if limit and stats["parsed"] > limit:
                break
            try:
                start_ip = ipaddress.ip_address(start_s)
            except ValueError:
                stats["skipped"] += 1
                continue
            if start_ip.version == 4:
                end_int = int(start_ip) + count - 1
                if count <= 0 or end_int > 0xFFFFFFFF:
                    stats["skipped"] += 1
                    continue
                end_ip = ipaddress.IPv4Address(end_int)
                aligned, plen = cidr_aligned(int(start_ip), count)
            else:
                end_ip = ipaddress.IPv6Address(int(start_ip) + count - 1)
                aligned, plen = (False, None)
            batch.append(
                (
                    registry,
                    cc,
                    rtype,
                    str(start_ip),
                    str(end_ip),
                    plen if aligned else None,
                    count,
                    aligned,
                    os.path.basename(path),
                )
            )
            if len(batch) >= cfg.db_batch_size:
                cur.executemany(UPSERT_SQL, batch)
                stats["inserted"] += len(batch)
                batch.clear()
                if stats["parsed"] % 100000 == 0:
                    log.info("... обработано %d строк (%.1f сек)", stats["parsed"], time.time() - t0)
        if batch:
            cur.executemany(UPSERT_SQL, batch)
            stats["inserted"] += len(batch)
    stats["seconds"] = round(time.time() - t0, 1)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", default=None, help="URL источника (по умолчанию из конфига)")
    ap.add_argument("--file", default=None, help="Локальный уже скачанный файл (пропускает download)")
    ap.add_argument("--limit", type=int, default=None, help="Ограничить число строк (для теста)")
    ap.add_argument("--skip-schema", action="store_true")
    ap.add_argument("--force-download", action="store_true",
                    help="скачать RIPE-файл принудительно, игнорируя кэш/TTL/хеш")
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="fetch")
    os.makedirs(cfg.data_dir, exist_ok=True)
    dest = args.file or os.path.join(cfg.data_dir, "delegated-ripencc-latest")

    if not args.skip_schema:
        init_schema()

    if not args.file:
        fetch(args.url or cfg.ripe_url, dest,
              force=args.force_download or cfg.ripe_force_download,
              ttl_hours=cfg.ripe_cache_ttl_hours)

    try:
        stats = load_into_db(cfg, dest, args.limit)
    except Exception as exc:  # noqa: BLE001
        log.exception("этап fetch завершился с ошибкой")
        stage_fail("fetch", error=repr(exc))
        raise
    log.info("Готово: %s", stats)
    stage_done("fetch", parsed=stats.get("parsed", "?"), inserted=stats.get("inserted", "?"),
               skipped=stats.get("skipped", "?"))
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
