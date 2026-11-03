#!/usr/bin/env python3
"""Сводный отчёт по пайплайну: сколько диапазонов/шардов/PTR/A/веб/OWA.

Использование:
    python -m ptr2.scripts.07_report [--format table|csv]
"""

from __future__ import annotations

import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.common import db_conn  # noqa: E402
from ptr2.logsetup import setup_logging, stage_done  # noqa: E402
from ptr2.config import get_config  # noqa: E402

QUERIES = [
    ("Диапазоны RIPE (ipv4) всего", "SELECT count(*), COALESCE(sum(size),0) FROM ripe_ranges WHERE type='ipv4'"),
    ("Шарды: все / done / pending", "SELECT count(*) FILTER (WHERE true), count(*) FILTER (WHERE status='done'), count(*) FILTER (WHERE status IN ('pending','queued')) FROM shards"),
    ("PTR-записей найдено", "SELECT count(*), COUNT(DISTINCT hostname) FROM ptr_records"),
    ("A-проверок: есть A / указывает на тот же IP", "SELECT count(*) FILTER (WHERE has_a_record), count(*) FILTER (WHERE points_back) FROM a_record_checks"),
    ("Веб-серверов доступно", "SELECT count(DISTINCT hostname) FROM web_checks WHERE reachable"),
    ("Обнаружено OWA (high confidence)", "SELECT count(DISTINCT hostname) FROM owa_checks WHERE is_owa AND confidence='high'"),
    ("Обнаружено OWA (любая уверенность)", "SELECT count(DISTINCT hostname) FROM owa_checks WHERE is_owa"),
]

TOP_OWA_SQL = """
SELECT oc.hostname, oc.probe_url, oc.confidence, oc.evidence,
       oc.exchange_version, oc.build_number, oc.version_source
  FROM owa_checks oc
  JOIN (SELECT hostname, max(id) mid FROM owa_checks WHERE is_owa GROUP BY hostname) t
    ON t.mid = oc.id
 ORDER BY CASE oc.confidence WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END
 LIMIT 50
"""

OWA_VERSIONS_SQL = """
SELECT COALESCE(exchange_version, '(не определена)') AS ver,
       count(DISTINCT hostname) AS hosts
  FROM owa_checks
 WHERE is_owa
 GROUP BY 1
 ORDER BY hosts DESC
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--format", choices=["table", "csv"], default="table")
    args = ap.parse_args()

    cfg = get_config()
    setup_logging(cfg.log_level, name="report")

    rows = []
    with db_conn(cfg.dsn()) as conn, conn.cursor() as cur:
        for title, sql in QUERIES:
            cur.execute(sql)
            rows.append((title, cur.fetchone()))
        cur.execute(TOP_OWA_SQL)
        top = cur.fetchall()
        cur.execute(OWA_VERSIONS_SQL)
        versions = cur.fetchall()
        cur.close()

    if args.format == "csv":
        w = csv.writer(sys.stdout)
        w.writerow(["metric", "value1", "value2"])
        for title, vals in rows:
            w.writerow([title, *vals])
        w.writerow([])
        w.writerow(["owa_host", "probe_url", "confidence", "evidence",
                    "exchange_version", "build_number", "version_source"])
        for r in top:
            w.writerow(list(r))
        w.writerow([])
        w.writerow(["exchange_version", "hosts"])
        for v, n in versions:
            w.writerow([v, n])
    else:
        print("=" * 78)
        print("СВОДНЫЙ ОТЧЁТ ptr2")
        print("=" * 78)
        for title, vals in rows:
            print(f"{title:<55} {tuple(v for v in vals)}")
        print("-" * 78)
        print("ВЕРСИИ ОБНАРУЖЕННОГО EXCHANGE/OWA (версия | хостов):")
        for v, n in versions:
            print(f"  {v:<45} {n}")
        if not versions:
            print("  (пусто)")
        print("-" * 78)
        print("TOP OWA (hostname | probe_url | confidence | version | evidence):")
        for h, p, c, e, ver, build, vsrc in top:
            vs = f"{ver}" + (f" [{build}]" if build and build not in (ver or "") else "")
            print(f"  {h:<40} {c:<8} {vs:<45} {p}")
            print(f"      evidence: {(e or '')[:100]}  src: {(vsrc or '')[:40]}")
        if not top:
            print("  (пусто)")
    stage_done("report", format=args.format, owa_versions=len(versions), top_owa=len(top))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
