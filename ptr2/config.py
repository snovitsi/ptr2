"""Конфигурация проекта: читается из переменных окружения / файла .env.

Пример .env:
    DATABASE_URL=postgresql+psycopg://ptr2:ptr2@localhost:5432/ptr2
    REDIS_URL=redis://localhost:6379/0
    RIPE_URL=https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-latest
    TARGET_COUNTRIES=DE,NL,FR
    MIN_PREFIX_LEN=24
    MAX_PREFIX_LEN=24
    SHARD_SIZE=256
    PTR_CONCURRENCY=100
    ...
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env(key: str, default):
    v = os.environ.get(key)
    if v is None or v.strip() == "":
        return default
    return v.strip()


def _env_int(key: str, default: int) -> int:
    return int(_env(key, default))


def _env_float(key: str, default: float) -> float:
    return float(_env(key, default))


def _env_bool(key: str, default: bool) -> bool:
    v = str(_env(key, "yes" if default else "no")).lower()
    return v in ("1", "true", "yes", "y", "on")


def _default_env_path() -> str:
    """Каталог корня проекта (где лежит пакет ptr2) — чтобы .env находился
    независимо от текущей директории запуска."""
    here = os.path.dirname(os.path.abspath(__file__))     # .../ptr2
    return os.path.join(os.path.dirname(here), ".env")    # .../<root>/.env


def _load_dotenv(path: str | None = None) -> None:
    """Мини-аналог python-dotenv: KEY=VALUE построчно, не перетирает окружение.

    Ищет .env в: $PTR2_ENV_FILE -> CWD -> корень проекта. Первый найденный
    файл применяется; переменные окружения имеют приоритет (setdefault)."""
    candidates = []
    if path:
        candidates.append(path)
    env_file = os.environ.get("PTR2_ENV_FILE")
    if env_file:
        candidates.append(env_file)
    candidates.append(os.path.join(os.getcwd(), ".env"))
    candidates.append(_default_env_path())
    for cand in candidates:
        if cand and os.path.exists(cand):
            path = cand
            break
    else:
        return
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            os.environ.setdefault(k, v)


_load_dotenv()  # при импорте config: переменные из .env попадут в os.environ до создания Config


@dataclass(frozen=True)
class Config:
    # --- внешние источники -------------------------------------------------
    ripe_url: str = field(
        default_factory=lambda: _env(
            "RIPE_URL",
            "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-latest",
        )
    )
    data_dir: str = field(default_factory=lambda: _env("DATA_DIR", "./data"))

    # --- критерии отбора диапазонов ---------------------------------------
    target_countries: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            c.strip().upper()
            for c in _env("TARGET_COUNTRIES", "").split(",")
            if c.strip()
        )
    )
    min_prefix_len: int = field(default_factory=lambda: _env_int("MIN_PREFIX_LEN", 24))
    max_prefix_len: int = field(default_factory=lambda: _env_int("MAX_PREFIX_LEN", 24))
    min_range_size: int = field(default_factory=lambda: _env_int("MIN_RANGE_SIZE", 0))
    max_range_size: int = field(default_factory=lambda: _env_int("MAX_RANGE_SIZE", 0))

    # --- шардирование ------------------------------------------------------
    shard_size: int = field(default_factory=lambda: _env_int("SHARD_SIZE", 256))

    # --- рассеивание по подсетям (анти-BAN DNS) -----------------------------
    # family_bits: префикс «семьи» (24 = /24). Шарды из разных семей чередуются
    # в очереди, чтобы одновременно не долбить одни и те же reverse-DNS-серверы.
    family_bits: int = field(default_factory=lambda: _env_int("FAMILY_BITS", 24))
    spread_enabled: bool = field(default_factory=lambda: _env_bool("SPREAD_ENABLED", True))
    # максимум ОДНОВРЕМЕННО обрабатываемых шардов (семей) на весь кластер воркеров
    spread_max_concurrent: int = field(
        default_factory=lambda: _env_int("SPREAD_MAX_CONCURRENT", 8)
    )
    # TTL аренды слота семьи, сек (упавший воркер освобождает слот сам)
    spread_lease_sec: int = field(default_factory=lambda: _env_int("SPREAD_LEASE_SEC", 900))
    key_shard_spread: str = field(
        default_factory=lambda: _env("KEY_SHARD_SPREAD", "ptr2:lock:shard_families")
    )
    # перемешивать адреса внутри шарда (детерминированный шифт по family_key)
    ptr_shuffle_within_shard: bool = field(
        default_factory=lambda: _env_bool("PTR_SHUFFLE_WITHIN_SHARD", True)
    )

    # --- хранилища ---------------------------------------------------------
    database_url: str = field(
        default_factory=lambda: _env(
            "DATABASE_URL",
            "postgresql+psycopg://ptr2:ptr2@localhost:5432/ptr2",
        )
    )
    redis_url: str = field(default_factory=lambda: _env("REDIS_URL", "redis://localhost:6379/0"))

    # --- DNS ----------------------------------------------------------------
    dns_timeout: float = field(default_factory=lambda: _env_float("DNS_TIMEOUT", 2.0))
    dns_lifetime: float = field(default_factory=lambda: _env_float("DNS_LIFETIME", 3.0))
    dns_nameservers: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            s.strip() for s in _env("DNS_NAMESERVERS", "").split(",") if s.strip()
        )
    )
    ptr_concurrency: int = field(default_factory=lambda: _env_int("PTR_CONCURRENCY", 100))
    ptr_rate_limit: float = field(default_factory=lambda: _env_float("PTR_RATE_LIMIT", 200.0))
    skip_network_and_broadcast: bool = field(
        default_factory=lambda: _env_bool("SKIP_NET_AND_BROADCAST", True)
    )

    # --- HTTP (A-записи и веб-проверки) ------------------------------------
    a_record_concurrency: int = field(default_factory=lambda: _env_int("A_CONCURRENCY", 50))
    http_concurrency: int = field(default_factory=lambda: _env_int("HTTP_CONCURRENCY", 30))
    http_timeout: float = field(default_factory=lambda: _env_float("HTTP_TIMEOUT", 10.0))
    user_agent: str = field(
        default_factory=lambda: _env(
            "USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        )
    )
    owa_paths: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            p.strip()
            for p in _env(
                "OWA_PATHS", "/owa/,/owa/auth/logon.aspx,/autodiscover/autodiscover.xml"
            ).split(",")
            if p.strip()
        )
    )
    verify_tls: bool = field(default_factory=lambda: _env_bool("VERIFY_TLS", False))

    # --- очереди Redis ------------------------------------------------------
    queue_shards_ptr: str = field(
        default_factory=lambda: _env("QUEUE_SHARDS_PTR", "ptr2:queue:ptr_shards")
    )
    queue_ptr_results: str = field(
        default_factory=lambda: _env("QUEUE_PTR_RESULTS", "ptr2:queue:ptr_results")
    )
    queue_a_checks: str = field(
        default_factory=lambda: _env("QUEUE_A_CHECKS", "ptr2:queue:a_checks")
    )
    queue_web_checks: str = field(
        default_factory=lambda: _env("QUEUE_WEB_CHECKS", "ptr2:queue:web_checks")
    )
    queue_owa_checks: str = field(
        default_factory=lambda: _env("QUEUE_OWACHK_CHECKS", "ptr2:queue:owa_checks")
    )

    # --- Redis Streams (распределённый режим: много python-хостов) ----------
    streams_enabled: bool = field(default_factory=lambda: _env_bool("USE_STREAMS", True))
    stream_ptr_shards: str = field(
        default_factory=lambda: _env("STREAM_PTR_SHARDS", "ptr2:stream:ptr_shards")
    )
    stream_ptr_results: str = field(
        default_factory=lambda: _env("STREAM_PTR_RESULTS", "ptr2:stream:ptr_results")
    )
    stream_a_checks: str = field(
        default_factory=lambda: _env("STREAM_A_CHECKS", "ptr2:stream:a_checks")
    )
    stream_web_checks: str = field(
        default_factory=lambda: _env("STREAM_WEB_CHECKS", "ptr2:stream:web_checks")
    )
    stream_owa_checks: str = field(
        default_factory=lambda: _env("STREAM_OWACHK_CHECKS", "ptr2:stream:owa_checks")
    )
    consumer_group: str = field(default_factory=lambda: _env("CONSUMER_GROUP", "workers"))
    # воркер считает задачу «зависшей» (упал хост) после простоя без ack; мс
    claim_idle_ms: int = field(default_factory=lambda: _env_int("CLAIM_IDLE_MS", 120000))
    worker_id: str = field(default_factory=lambda: _env("WORKER_ID", ""))  # пусто = hostname:pid
    # --- служебное ----------------------------------------------------------
    db_batch_size: int = field(default_factory=lambda: _env_int("DB_BATCH_SIZE", 1000))
    # логирование: см. ptr2/logsetup.py (файлы логов + общий pipeline.log)
    log_level: str = field(default_factory=lambda: _env("LOG_LEVEL", "INFO"))
    log_dir: str = field(default_factory=lambda: _env("LOG_DIR", ""))
    log_to_file: bool = field(default_factory=lambda: _env_bool("LOG_TO_FILE", True))

    def dsn(self) -> str:
        """DSN для psycopg 3: убираем из схемы префикс драйвера (+psycopg/+asyncpg)."""
        scheme, sep, rest = self.database_url.partition("://")
        if not sep:
            return self.database_url
        if "+" in scheme:
            scheme = scheme.split("+", 1)[0] or "postgresql"
        if scheme in ("postgres", ""):
            scheme = "postgresql"
        return f"{scheme}://{rest}"


@lru_cache(maxsize=1)
def get_config() -> Config:
    return Config()


# ---------------------------------------------------------------------------
# Хелперы очередей: единая точка выбора транспорта (Streams / legacy-списки).
# ---------------------------------------------------------------------------

def _redis_for(url: str | None):
    from .redisq import get_redis

    cfg = get_config()
    return get_redis(url or cfg.redis_url)


def producer(queue: str, redis_url: str | None = None, *, use_streams: bool | None = None):
    """Один интерфейс для продюсера этапа.

    use_streams=None -> решение принимает Config.streams_enabled (USE_STREAMS).
    Возвращает StreamProducer (есть send_batch) или SimpleQueue (push_batch).
    """
    cfg = get_config()
    if cfg.streams_enabled if use_streams is None else use_streams:
        from .redisq import StreamProducer

        return StreamProducer(_redis_for(redis_url), queue)
    from .redisq import SimpleQueue

    return SimpleQueue(_redis_for(redis_url), queue)


def _consumer_name(cfg: "Config", suffix: str = "") -> str:
    """Уникальное имя потребителя: hostname:pid + WORKER_TAG + суффикс этапа.

    WORKER_TAG (-01/-02...) задаётся run_pipeline.sh для параллельных воркеров
    одного хоста; pid уникален лишь внутри хоста, тег делает имя различимым в
    XINFO GROUPS/PEL и защищает от коллизий при переиспользовании pid.
    """
    name = cfg.worker_id or f"{__import__('socket').gethostname()}:{os.getpid()}"
    tag = os.environ.get("WORKER_TAG", "")
    if tag:
        name = f"{name}{tag}"
    if suffix:
        name = f"{name}-{suffix}"
    return name


def consumer(queue: str, group: str | None = None, suffix: str = "",
             redis_url: str | None = None, *, use_streams: bool | None = None):
    """Один интерфейс для консьюмера этапа (StreamConsumer или SimpleQueue)."""
    cfg = get_config()
    if cfg.streams_enabled if use_streams is None else use_streams:
        from .redisq import StreamConsumer

        return StreamConsumer(
            _redis_for(redis_url), queue, group or cfg.consumer_group,
            consumer=_consumer_name(cfg, suffix), idle_timeout_ms=cfg.claim_idle_ms,
        )
    from .redisq import SimpleQueue

    return SimpleQueue(_redis_for(redis_url), queue)


# --- обратная совместимость (старые имена функций) ---------------------------

def stream_producer(stream: str, redis_url: str | None = None):
    """Deprecated: используйте producer(). Всегда Streams."""
    return producer(stream, redis_url, use_streams=True)


def stream_consumer(stream: str, group: str | None = None, suffix: str = "",
                    redis_url: str | None = None):
    """Deprecated: используйте consumer(). Всегда Streams."""
    return consumer(stream, group, suffix, redis_url, use_streams=True)
