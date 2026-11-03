"""Асинхронный DNS: PTR (reverse) и A/AAAA резолвинг поверх dnspython + asyncio.

Используется thread pool-обёртка над dns.resolver (у sync-резолвера нет нативного async API
в dnspython 2.x без trio/curio), либо dns.asyncresolver. Здесь — dns.asyncresolver
с собственным ResolverConfiguration и пулом семафоров + rate limiter.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import time
from typing import Iterable

import dns.asyncresolver
import dns.exception
import dns.flags
import dns.message
import dns.name
import dns.rdatatype
import dns.reversename

log = logging.getLogger("ptr2.dns")


def normalize_hostname(name) -> str:
    """dns.name.Name -> lowercase string без конечной точки."""
    s = str(name).strip(".").lower()
    return s


class DnsClient:
    def __init__(
        self,
        nameservers: Iterable[str] | None = None,
        timeout: float = 2.0,
        lifetime: float = 3.0,
        concurrency: int = 100,
        rate_limit: float = 200.0,
    ):
        self.resolver = dns.asyncresolver.Resolver()
        ns = [n for n in (nameservers or []) if n]
        if ns:
            self.resolver.nameservers = ns
        self.resolver.timeout = timeout
        self.resolver.lifetime = lifetime
        self._sem = asyncio.Semaphore(concurrency)
        self._rate = rate_limit  # запросов в секунду
        self._tokens = rate_limit
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def _throttle(self) -> None:
        if not self._rate or self._rate <= 0:
            return
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self._rate, self._tokens + (now - self._last_refill) * self._rate)
                self._last_refill = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self._rate)

    async def query_ptr(self, ip: str) -> list[str]:
        """Все PTR-имена для IPv4/IPv6 адреса. [] если записи нет / ошибка."""
        try:
            addr = ipaddress.ip_address(ip)
        except ValueError:
            return []
        rev = dns.reversename.from_address(str(addr))
        async with self._sem:
            await self._throttle()
            try:
                ans = await self.resolver.resolve(rev, "PTR")
            except (
                dns.asyncresolver.NXDOMAIN,
                dns.asyncresolver.NoAnswer,
                dns.exception.DNSException,
                asyncio.TimeoutError,
                OSError,
            ):
                return []
            out = []
            for rd in ans:
                try:
                    host = normalize_hostname(rd.target)
                    if host:
                        out.append(host)
                except Exception:
                    continue
            return out

    async def query_a(self, hostname: str) -> list[str]:
        """A (+AAAA на всякий случай) записи хостнейма. [] если нет/ошибка."""
        results: list[str] = []
        async with self._sem:
            await self._throttle()
            try:
                for rd in await self.resolver.resolve(hostname, "A"):
                    results.append(rd.address)
            except (
                dns.asyncresolver.NXDOMAIN,
                dns.asyncresolver.NoAnswer,
                dns.exception.DNSException,
                asyncio.TimeoutError,
                OSError,
            ):
                pass
        return results

    async def query_any_a_or_aaaa(self, hostname: str) -> tuple[list[str], list[str]]:
        a = await self.query_a(hostname)
        aaaa: list[str] = []
        async with self._sem:
            await self._throttle()
            try:
                for rd in await self.resolver.resolve(hostname, "AAAA"):
                    aaaa.append(rd.address)
            except Exception:
                pass
        return a, aaaa


def iter_ips(start: str, end: str, skip_network_and_broadcast: bool = False) -> Iterable[str]:
    """Генератор IPv4-адресов диапазона [start..end] включительно."""
    a = int(ipaddress.IPv4Address(start))
    b = int(ipaddress.IPv4Address(end))
    for i in range(a, b + 1):
        if skip_network_and_broadcast:
            if i % 256 == 0 or i % 256 == 255:
                continue
        yield str(ipaddress.IPv4Address(i))
