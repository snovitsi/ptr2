"""Общие утилиты: CIDR-разбиение, DB-сессии (логирование — в ptr2/logsetup.py).

Формат delegated-ripencc-latest (pipe-delimited):
  registry|cc|type|start|count|asn|last|status|magic
где count — количество адресов для ipv4/ipv6 (не префикс!), либо ASN.
"""

from __future__ import annotations

import ipaddress
import logging
import sys
from contextlib import contextmanager
from typing import Iterator

import psycopg


@contextmanager
def db_conn(dsn: str) -> Iterator[psycopg.Connection]:
    """Транзакционнаяconn-сессия: commit при успехе, rollback при исключении."""
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def split_to_cidr(start: int, size: int) -> list[str]:
    """Разбивает произвольный диапазон [start .. start+size-1] на список CIDR."""
    out: list[str] = []
    end = start + size - 1
    cur = start
    while cur <= end:
        # максимальный блок, выровненный по cur и не выходящий за end
        max_bits = 32
        while max_bits > 0:
            blk_size = 1 << max_bits
            if cur % blk_size == 0 and cur + blk_size - 1 <= end:
                break
            max_bits -= 1
        blk_size = 1 << max_bits
        out.append(str(ipaddress.IPv4Network((cur, 32 - max_bits), strict=False)))
        cur += blk_size
    return out


def cidr_aligned(start: int, size: int) -> tuple[bool, int | None]:
    """Выровнен ли диапазон на границу CIDR; возвращает (aligned, prefix_len)."""
    if size <= 0 or (size & (size - 1)) != 0:  # размер не степень двойки
        return False, None
    bits = size.bit_length() - 1
    if start % size != 0:
        return False, None
    return True, 32 - bits


def parse_hostname_from_url(url: str) -> str | None:
    from urllib.parse import urlparse

    host = urlparse(url).hostname
    return host.lower() if host else None
