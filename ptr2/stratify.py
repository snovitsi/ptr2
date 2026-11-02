"""Стратификация (рассеивание) шардов по /24-подсетям — защита от BAN DNS-серверов.

Проблема: если ставить задачи в очередь подряд (все шарды одного диапазона /
соседние /24 одной сети), воркеры одновременно опрашивают адреса, чьи reverse-зоны
делегируются на ОДНИ И ТЕ ЖЕ авторитетные DNS-серверы провайдера. Большой поток
запросов к одним серверам -> rate-limit / бан нашего резолвера.

Решение (два уровня):

1. На этапе шардинга (01_select_and_shard.py):
   * каждый шард получает "семью" — сетевой префикс (/24 по умолчанию,
     column family_key в таблице shards);
   * порядок раздачи задач = round-robin по семьям: сначала один шард из каждой
     /24, затем второй и т.д. Соседние по очереди задачи — из РАЗНЫХ подсетей
     (разных reverse-зон / разных DNS-серверов).

2. На этапе PTR-опроса (02_ptr_worker.py):
   * адреса внутри шарда перемешиваются детерминированным шифтом (keystream от
     MD5(family_key)) — даже два шарда одной /24 опрашиваются в разном порядке;
   * глобальный через Redis лимит (KEY_SHARD_SPREAD) ограничивает число
     шардов, обрабатываемых ОДНОВРЕМЕННО всеми воркерами со всего кластера —
     не более SPREAD_MAX_CONCURRENT семей одновременно. Т.к. очередь уже
     чередует семьи, это означает: в любой момент времени N воркеров работают
     над N РАЗНЫМИ /24, а не долбят одну сеть.
"""

from __future__ import annotations

import hashlib
import ipaddress
import time
from typing import Iterable


def family_of(start_ip: str, end_ip: str, bits: int = 24) -> str:
    """Сетевой префикс (key) подсети, к которой относится шард.

    Берётся start шарда; если шард пересекает несколько /bits (для выровненных
    CIDR-шардов размер <= 2^host-bits, так что обычно не пересекает) — всё равно
    считаем семью по start. Для IPv6 family = сам шард (одна reverse-зона).
    """
    try:
        a = ipaddress.IPv4Address(start_ip)
    except ValueError:
        # IPv6 или мусор: своя «семья» на шард — перемешивать нечего
        return f"single:{start_ip}-{end_ip}"
    net = ipaddress.IPv4Network((int(a), bits), strict=False)
    return str(net)


def shuffled_indices(n: int, key: str) -> list[int]:
    """Перемешивание range(n) детерминированным keystream'ом от key.

    Детерминизм важен: при перезапуске воркера задача обрабатывается в том же
    порядке. Разные key (разные /24) дают независимые перестановки.
    Fisher-Yates со случайностью из MD5(key || counter) — без secrets/PRNG.
    """
    idx = list(range(n))
    if n <= 1:
        return idx
    stream: list[int] = []

    def rnds(count: int) -> Iterable[int]:
        c = 0
        while len(stream) < count:
            block = hashlib.md5(f"{key}:{c}".encode()).digest()
            stream.extend(block)
            c += 1
        out, stream[:] = stream[:count], stream[count:]
        return out

    need = (n - 1) * 4
    it = iter(rnds(need))
    for i in range(n - 1, 0, -1):
        j = int.from_bytes(bytes([next(it), next(it), next(it), next(it)]), "big") % (i + 1)
        idx[i], idx[j] = idx[j], idx[i]
    return idx


def interleave_by_family(tasks: list[dict], key_field: str = "family_key") -> list[dict]:
    """Round-robin по семьям: [A1,B1,C1, A2,B2,C2, ...].

    Внутри каждой семьи задачи упорядочены детерминированно (range_id, shard_index),
    но семьи между собой чередуются, поэтому любые SPREAD_MAX_CONCURRENT соседних
    задач в очереди — из разных подсетей.
    """
    groups: dict[str, list[dict]] = {}
    for t in tasks:
        groups.setdefault(str(t.get(key_field) or ""), []).append(t)
    for g in groups.values():
        g.sort(key=lambda t: (int(t.get("range_id") or 0), int(t.get("shard_index") or 0)))
    order = sorted(groups)  # стабильный порядок семей между запусками
    out: list[dict] = []
    pos = 0
    while True:
        added = False
        for fam in order:
            g = groups[fam]
            if pos < len(g):
                out.append(g[pos])
                added = True
        if not added:
            break
        pos += 1
    return out


class SpreadGuard:
    """Глобальный (через Redis) лимит одновременно активных «семей» шардов.

    Семафор на N слотов + реестр занятых семей:
      * если семья уже занята другим воркером (с любого хоста) — новый шард
        ждёт, пока она освободится (не долбим одну /24 с двух мест сразу);
      * если все слоты заняты РАЗНЫМИ семьями и достигнут SPREAD_MAX_CONCURRENT,
        шард из НОВОЙ семьи тоже подождёт — очередь при этом всё ещё идёт
        round-robin, поэтому ожидаются разные подсети, а не одна сеть.

    Реализация: sorted set (член = f"{consumer}:{token}", score = deadline).
    Упавшие воркеры автоматически выбывают по TTL (cleanup удаляет протухшие).
    """

    def __init__(self, r, key: str, max_families: int, lease_sec: int = 900,
                 consumer: str = "w"):
        self.r = r
        self.key = key
        self.max_families = max(1, int(max_families))
        self.lease_sec = lease_sec
        self.consumer = consumer
        self._held: dict[str, str] = {}  # family -> token

    # скрипт: cleanup + попытка занять семью атомарно.
    # KEYS[1]=zset; ARGV[1]=now, ARGV[2]=deadline, ARGV[3]=family,
    # ARGV[4]=member, ARGV[5]=max_families
    _ACQUIRE_LUA = """
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
    local members = redis.call('ZRANGE', KEYS[1], 0, -1)
    local fam = ARGV[3]
    local nfam = 0
    local seen = {}
    for i = 1, #members do
      local m = members[i]
      local fpos = string.find(m, '|', 1, true)
      local f = string.sub(m, 1, fpos - 1)
      if f == fam then
        return -1  -- семья занята другим воркером
      end
      if not seen[f] then seen[f] = true; nfam = nfam + 1 end
    end
    if nfam >= tonumber(ARGV[5]) then
      return -2  -- лимит одновременных семей исчерпан
    end
    redis.call('ZADD', KEYS[1], ARGV[2], fam .. '|' .. ARGV[4])
    return 1
    """

    def try_acquire(self, family: str) -> bool:
        import uuid

        token = f"{self.consumer}|{uuid.uuid4().hex[:12]}"
        now = time.time()
        res = self.r.eval(
            self._ACQUIRE_LUA, 1, self.key,
            now, now + self.lease_sec, family, token, self.max_families,
        )
        if res == 1:
            self._held[family] = token
            return True
        return False

    def release(self, family: str) -> None:
        token = self._held.pop(family, None)
        if token is not None:
            self.r.zrem(self.key, f"{family}|{token}")

    def active_families(self) -> list[str]:
        now = time.time()
        self.r.zremrangebyscore(self.key, "-inf", now)
        fams = set()
        for m in self.r.zrange(self.key, 0, -1):
            fams.add(m.split("|", 1)[0])
        return sorted(fams)

