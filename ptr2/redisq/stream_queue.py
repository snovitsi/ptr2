"""Redis Streams: надёжные очереди для распределённого запуска на нескольких серверах.

В отличие от списковой очереди (BLMOVE) из redisq/list_queue.py, Stream + Consumer Group даёт:
  * параллельное потребление многими воркерами с разных хостов без потерь;
  * гарантию доставки (at-least-once): неподтверждённые задачи остаются в PEL
    группы и могут быть перехвачены любым живым потребителем (XAUTOCLAIM/XCLAIM);
  * наблюдаемость: XINFO GROUPS показывает lag, XLEN — размер очереди.

Использование:
    sp = StreamProducer(r, "ptr2:stream:ptr_shards")
    sp.send_batch([{"shard_id": 1, ...}, ...])          # XADD (id возвращаются сами)

    sc = StreamConsumer(r, "ptr2:stream:ptr_shards", group="workers",
                        consumer=f"{socket.gethostname()}:{pid}")
    for msg_id, fields in sc.claim_stale(min_idle_ms=120000):   # восстановление зависших
        ...
        sc.ack(msg_id)
    for msg_id, fields in sc.read(count=50, block_ms=2000):     # новые задачи
        ...
        sc.ack(msg_id)

payload передаётся dict'ом; значения сериализуются в строки (JSON для сложных типов).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from typing import Iterable, Iterator

import redis

log = logging.getLogger("ptr2.stream")

# префикс поля, в котором лежит JSON-сериализованный payload (для вложенных структур)
_PAYLOAD_KEY = "_json"


def _encode(payload: dict) -> dict[str, str]:
    """dict -> flat {str: str}: простые скаляры как есть, остальное в JSON-блок."""
    flat: dict[str, str] = {}
    complex_part: dict[str, object] = {}
    for k, v in payload.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            flat[k] = "" if v is None else str(v)
        else:
            complex_part[k] = v
    if complex_part:
        flat[_PAYLOAD_KEY] = json.dumps(complex_part, ensure_ascii=False)
    return flat


def _decode(fields: dict[str, str]) -> dict:
    out = {k: v for k, v in fields.items() if k != _PAYLOAD_KEY}
    raw = fields.get(_PAYLOAD_KEY)
    if raw:
        try:
            out.update(json.loads(raw))
        except (ValueError, TypeError):
            log.warning("Не удалось декодировать JSON-часть сообщения: %r", raw[:200])
    return out


class StreamProducer:
    def __init__(self, r: redis.Redis, stream: str, maxlen: int | None = 100_000):
        self.r = r
        self.stream = stream
        self.maxlen = maxlen  # примерный trim (XADD ... MAXLEN ~N), чтобы Redis не раздувался

    def send(self, payload: dict) -> str:
        kwargs = {"nomkstream": True}
        if self.maxlen:
            kwargs["maxlen"] = self.maxlen
            kwargs["approx"] = True
        return self.r.xadd(self.stream, _encode(payload), **kwargs)

    def send_batch(self, payloads: list[dict], chunk: int = 500) -> int:
        n = 0
        pipe = self.r.pipeline(transaction=False)
        for p in payloads:
            kwargs = {"nomkstream": True}
            if self.maxlen:
                kwargs["maxlen"] = self.maxlen
                kwargs["approx"] = True
            pipe.xadd(self.stream, _encode(p), **kwargs)
            n += 1
            if n % chunk == 0:
                pipe.execute()
        pipe.execute()
        return n


class StreamConsumer:
    """Один потребитель внутри группы; безопасен для сотен процессов на разных хостах."""

    def __init__(
        self,
        r: redis.Redis,
        stream: str,
        group: str,
        consumer: str | None = None,
        idle_timeout_ms: int = 120_000,
    ):
        self.r = r
        self.stream = stream
        self.group = group
        self.consumer = consumer or f"{socket.gethostname()}:{os.getpid()}"
        self.idle_timeout_ms = idle_timeout_ms

    def ensure_group(self) -> None:
        """Создаёт группу (идempotent). BusYGroup — ок, NOGROUP — создаём с начала."""
        try:
            self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            log.info("Создана consumer group %s для %s", self.group, self.stream)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def read(self, count: int = 50, block_ms: int = 2000) -> list[tuple[str, dict]]:
        """Новые сообщения группы (> last_id). Пустой список при таймауте блока."""
        try:
            resp = self.r.xreadgroup(
                self.group, self.consumer, {self.stream: ">"}, count=count, block=block_ms
            )
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                self.ensure_group()
                return []
            raise
        if not resp:
            return []
        _, entries = resp[0][0], resp[0][1]
        return [(mid, _decode(fields)) for mid, fields in entries]

    def pending_ids(self, count: int = 1000) -> list[str]:
        """ID сообщений в PEL этого потребителя (неподтверждённые)."""
        try:
            pend = self.r.xpending_range(
                self.stream, self.group, min="-", max="+", count=count
            )
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                return []
            raise
        return [p["message_id"] for p in pend]

    def claim_stale(self, min_idle_ms: int | None = None, count: int = 100) -> list[tuple[str, dict]]:
        """Перехват зависших сообщений ЛЮБЫХ потребителей группы (XAUTOCLAIM со стартом '0').

        Возвращает (id, payload); вызывающий обязан обработать и ack'нуть их.
        """
        idle = min_idle_ms if min_idle_ms is not None else self.idle_timeout_ms
        start = "0"  # каждый проход сканирует всю PEL группы — просто, но O(pending)
        try:
            claimed = self.r.xautoclaim(
                self.stream, self.group, self.consumer, min_idle_time=idle,
                start_id=start, count=count,
            )
        except redis.exceptions.ResponseError as e:
            if "NOGROUP" in str(e):
                return []
            if "unknown command" in str(e).lower():  # Redis < 6.2 — fallback вручную
                return self._claim_stale_fallback(idle, count)
            raise
        entries = claimed[1] if len(claimed) > 1 else []
        return [(mid, _decode(fields)) for mid, fields in entries]

    def _claim_stale_fallback(self, min_idle_ms: int, count: int) -> list[tuple[str, dict]]:
        """Для старых Redis: XPENDING + XCLAIM."""
        ids: list[str] = []
        cursor = "-"
        while len(ids) < count:
            try:
                pend = self.r.xpending_range(
                    self.stream, self.group, min=cursor, max="+", count=count
                )
            except redis.exceptions.ResponseError:
                break
            stale = [p for p in pend if p["time_since_delivered"] >= min_idle_ms]
            if not pend:
                break
            cursor = pend[-1]["message_id"]
            ids.extend(p["message_id"] for p in stale)
            if len(pend) < count:
                break
        ids = ids[:count]
        if not ids:
            return []
        try:
            claimed = self.r.xclaim(
                self.stream, self.group, self.consumer, min_idle_time=min_idle_ms,
                message_ids=ids,
            )
        except redis.exceptions.ResponseError:
            return []
        return [(mid, _decode(fields)) for mid, fields in claimed]

    def ack(self, *msg_ids: str) -> None:
        if msg_ids:
            self.r.xack(self.stream, self.group, *msg_ids)

    def release_unprocessed(self, *msg_ids: str) -> None:
        """Ничего не делает: сообщение остаётся в PEL этого потребителя.

        Используется, когда воркер НЕ смог приступить к задаче (например, ждёт
        освобождения /24-семьи в SpreadGuard): задача уже «нашлась» нам и будет
        перехвачена через min_idle_ms (claim_stale) — повторной обработки нет,
        но и в потерях тоже.
        """

    def depth(self) -> int:
        """Приблизительное число необработанных сообщений (Lag по XINFO GROUPS)."""
        try:
            for g in self.r.xinfo_groups(self.stream):
                if g["name"] in (self.group.encode(), self.group):
                    return int(g.get("lag") or 0)
        except redis.exceptions.ResponseError:
            pass
        return 0

    def pending_total(self) -> int:
        try:
            return int(self.r.xpending(self.stream, self.group)["pending"])
        except redis.exceptions.ResponseError:
            return 0


def default_consumer_name(suffix: str = "") -> str:
    return f"{socket.gethostname()}:{os.getpid()}{('-' + suffix) if suffix else ''}"


def wait_drain(consumers: Iterable[StreamConsumer], timeout_sec: float = 0,
               poll_sec: float = 2.0) -> bool:
    """Блокируется до опустошения очередей заданных потребителей (для run_all на одном хосте)."""
    deadline = time.monotonic() + timeout_sec if timeout_sec else None
    while True:
        busy = any(c.depth() or c.pending_total() for c in consumers)
        if not busy:
            return True
        if deadline and time.monotonic() > deadline:
            return False
        time.sleep(poll_sec)
