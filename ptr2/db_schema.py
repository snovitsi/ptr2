"""Инициализация PostgreSQL-схемы."""

from __future__ import annotations

import logging

import psycopg

from .config import get_config

log = logging.getLogger("ptr2.schema")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ripe_ranges (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    registry        TEXT NOT NULL,
    cc              TEXT,                      -- код страны (может быть A1/A2 и т.п.)
    type            TEXT NOT NULL,             -- ipv4 | ipv6 | asn
    start_ip        INET NOT NULL,
    end_ip          INET,
    prefix_len      INT,                       -- только для выровненных CIDR
    size            BIGINT NOT NULL,           -- количество адресов
    is_cidr_aligned BOOLEAN NOT NULL DEFAULT FALSE,
    source_file     TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (start_ip, end_ip, type)
);
CREATE INDEX IF NOT EXISTS idx_ripe_ranges_cc ON ripe_ranges (cc);
CREATE INDEX IF NOT EXISTS idx_ripe_ranges_size ON ripe_ranges (size);

-- шарды по shard_size адресов (по умолчанию 256)
CREATE TABLE IF NOT EXISTS shards (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    range_id     BIGINT NOT NULL REFERENCES ripe_ranges(id) ON DELETE CASCADE,
    shard_index  INT NOT NULL,
    start_ip     INET NOT NULL,
    end_ip       INET NOT NULL,
    family_key   TEXT,                          -- /24-префикс шарда (анти-BAN: задачи чередуются по семьям)
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending|queued|done|error
    queued_at    TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    UNIQUE (range_id, shard_index)
);
CREATE INDEX IF NOT EXISTS idx_shards_status ON shards (status);
CREATE INDEX IF NOT EXISTS idx_shards_family ON shards (family_key);

-- результаты PTR-опроса
CREATE TABLE IF NOT EXISTS ptr_records (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ip           INET NOT NULL,
    hostname     TEXT NOT NULL,               -- нормализован (lower, без точки в конце)
    raw_response TEXT,
    shard_id     BIGINT REFERENCES shards(id) ON DELETE SET NULL,
    found_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ip, hostname)
);
CREATE INDEX IF NOT EXISTS idx_ptr_hostname ON ptr_records (hostname);

-- проверка прямой A-записи для найденных хостнеймов
CREATE TABLE IF NOT EXISTS a_record_checks (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hostname     TEXT NOT NULL,
    expected_ip  INET NOT NULL,               -- IP, с которого найден этот хостнейм
    resolved_ips INET[],                       -- все A/AAAA, которые отдаёт DNS
    has_a_record BOOLEAN NOT NULL DEFAULT FALSE,
    points_back  BOOLEAN,                     -- есть ли среди resolved_ips тот же expected_ip
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (hostname, expected_ip)
);
CREATE INDEX IF NOT EXISTS idx_a_checks_has ON a_record_checks (has_a_record);

-- доступность веб-сервера по хостнейму
CREATE TABLE IF NOT EXISTS web_checks (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hostname       TEXT NOT NULL,
    url            TEXT NOT NULL,
    final_url      TEXT,
    http_status    INT,
    server_header  TEXT,
    title          TEXT,
    reachable      BOOLEAN NOT NULL DEFAULT FALSE,
    error          TEXT,
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (url)
);

-- обнаружение Outlook Web Access / Exchange + версия
CREATE TABLE IF NOT EXISTS owa_checks (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hostname       TEXT NOT NULL,
    url            TEXT NOT NULL,             -- базовый URL веб-сервера
    probe_url      TEXT,                      -- путь, где обнаружен OWA
    is_owa         BOOLEAN NOT NULL DEFAULT FALSE,
    confidence     TEXT,                      -- high|medium|low
    evidence       TEXT,                      -- что именно указало на OWA
    http_status    INT,
    redirect_chain TEXT,
    -- определение версии Exchange/OWA
    exchange_version TEXT,                    -- человекочитаемая: "Exchange 2016 CU23"
    build_number     TEXT,                    -- сырая сборка: "15.01.2507.017"
    version_source   TEXT,                    -- x-owa-version|feowaversion|msexccver|body-meta|...
    checked_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (hostname, url, probe_url)
);
CREATE INDEX IF NOT EXISTS idx_owa_is ON owa_checks (is_owa);
CREATE INDEX IF NOT EXISTS idx_owa_version ON owa_checks (exchange_version);

-- служебная таблица статусов пайплайна
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stage       TEXT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    stats       JSONB
);
"""


def init_schema() -> None:
    cfg = get_config()
    with psycopg.connect(cfg.dsn()) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        # мягкая миграция: family_key для старых БД (созданных до анти-BAN режима)
        cur.execute(
            """
            ALTER TABLE shards ADD COLUMN IF NOT EXISTS family_key TEXT;
            CREATE INDEX IF NOT EXISTS idx_shards_family ON shards (family_key);
            -- мягкая миграция: колонки версии Exchange/OWA
            ALTER TABLE owa_checks ADD COLUMN IF NOT EXISTS exchange_version TEXT;
            ALTER TABLE owa_checks ADD COLUMN IF NOT EXISTS build_number TEXT;
            ALTER TABLE owa_checks ADD COLUMN IF NOT EXISTS version_source TEXT;
            CREATE INDEX IF NOT EXISTS idx_owa_version ON owa_checks (exchange_version);
            """
        )
    log.info("Схема БД готова (postgres)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_schema()
    print("OK: схема инициализирована")
