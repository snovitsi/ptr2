"""Обёртка над Redis: надёжные очереди (BLMOVE / RPOPLPUSH) с видимостью задач.

Схема очередей:
  <queue>          — список "готовых к работе" задач
  <queue>:processing — список задач, взятых воркерами в работу

Возврат задачи в очередь ("ack"=False) нужен при падении воркера.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Iterator

import redis

log = logging.getLogger("ptr2.redisq.list")


class SimpleQueue:
    """Упрощённая надёжная очередь: задачи хранятся как JSON-строки.

    pop()  -> BLMOVE right->processing left
    ack(payload_raw) -> LREM processing
    """

    def __init__(self, r: redis.Redis, name: str):
        self.r = r
        self.name = name
        self.processing = f"{name}:processing"

    def push(self, payload: dict) -> None:
        self.r.rpush(self.name, json.dumps(payload, ensure_ascii=False))

    def push_batch(self, payloads: list[dict]) -> None:
        if not payloads:
            return
        pipe = self.r.pipeline(transaction=False)
        for p in payloads:
            pipe.rpush(self.name, json.dumps(p, ensure_ascii=False))
        pipe.execute()

    def pop(self, timeout: int = 3) -> tuple[str, dict] | None:
        raw = self.r.blmove(
            self.name, self.processing, timeout=timeout, src="RIGHT", dest="LEFT"
        )
        if raw is None:
            return None
        return raw, json.loads(raw)

    def ack(self, raw: str) -> None:
        self.r.lrem(self.processing, 1, raw)

    def done(self, raw: str) -> None:
        """Отметить обработку завершённой без возврата в ready (то же, что ack)."""
        self.r.lrem(self.processing, 1, raw)

    def requeue(self, raw: str) -> None:
        self.r.lrem(self.processing, 1, raw)
        self.r.rpush(self.name, raw)

    def depth(self) -> int:
        return self.r.llen(self.name)

    def processing_depth(self) -> int:
        return self.r.llen(self.processing)

    def reclaim_stale(self, max_idle_sec: int = 0) -> int:
        """Возврат всех зависших задач из processing в ready (для перезапуска)."""
        n = 0
        while True:
            raw = self.r.rpoplpush(self.processing, self.name)
            if raw is None:
                break
            n += 1
        return n

    def clear(self) -> None:
        self.r.delete(self.name, self.processing)


def get_redis(url: str | None = None) -> redis.Redis:
    from ..config import get_config

    url = url or get_config().redis_url
    r = redis.Redis.from_url(
        url,
        decode_responses=True,
        # для удалённого Redis: не «зависать» навечно при обрыве сети
        socket_connect_timeout=float(os.environ.get("REDIS_CONNECT_TIMEOUT", "5")),
        socket_keepalive=True,
        health_check_interval=int(os.environ.get("REDIS_HEALTH_CHECK_INTERVAL", "30")),
        retry_on_timeout=True,
    )
    r.ping()
    return r


# --- Обратная совместимость с legacy-именами списковых очередей ---------------
# В старой схеме использовались имена "ptr2:queue:*"; в новой по умолчанию
# применяются Streams ("ptr2:stream:*"). Хелпер ниже позволяет запускать
# воркеры против старых списковых очередей без правки .env.
LEGACY_QUEUE_MAP = {
    "ptr_shards": "ptr2:queue:ptr_shards",
    "ptr_results": "ptr2:queue:ptr_results",
    "a_checks": "ptr2:queue:a_checks",
    "web_checks": "ptr2:queue:web_checks",
    "owa_checks": "ptr2:queue:owa_checks",
}


def legacy_queue_name(short: str) -> str:
    """Короткое имя этапа -> полное имя legacy-очереди (списковой)."""
    return LEGACY_QUEUE_MAP.get(short, f"ptr2:queue:{short}")


def heartbeat(r: redis.Redis, key: str, ttl: int = 60) -> None:
    r.set(key, str(time.time()), ex=ttl)


def is_alive(r: redis.Redis, key: str) -> bool:
    return bool(r.exists(key))
