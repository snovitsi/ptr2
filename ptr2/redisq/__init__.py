"""Очереди Redis в двух реализациях (единый интерфейс get_redis):

* list_queue.py  — надёжные списковые очереди (BLMOVE/RPOPLPUSH, SimpleQueue),
                   режим одной машины / legacy;
* stream_queue.py — Redis Streams + Consumer Groups (StreamProducer/StreamConsumer),
                   распределённый режим на N хостах (используется по умолчанию,
                   см. USE_STREAMS).
"""

from __future__ import annotations

from .list_queue import (
    LEGACY_QUEUE_MAP,
    SimpleQueue,
    get_redis,
    heartbeat,
    is_alive,
    legacy_queue_name,
)
from .stream_queue import (
    StreamConsumer,
    StreamProducer,
    default_consumer_name,
    wait_drain,
)

__all__ = [
    "SimpleQueue",
    "StreamProducer",
    "StreamConsumer",
    "get_redis",
    "heartbeat",
    "is_alive",
    "legacy_queue_name",
    "LEGACY_QUEUE_MAP",
    "default_consumer_name",
    "wait_drain",
]
