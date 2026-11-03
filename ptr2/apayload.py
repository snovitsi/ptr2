"""Формат сообщений stream a_checks: пачка пар (hostname, expected_ip) в JSON."""

from __future__ import annotations

import json
import logging

log = logging.getLogger("ptr2.apayload")

PAIRS_KEY = "pairs"


def encode_pairs(pairs: list[tuple[str, str]]) -> dict:
    """Несколько пар в одном сообщении stream (меньше сообщений — легче load)."""
    return {PAIRS_KEY: json.dumps([list(p) for p in pairs])}


def decode_pairs(payload: dict) -> list[tuple[str, str]]:
    raw = payload.get(PAIRS_KEY) or "[]"
    try:
        return [tuple(x) for x in json.loads(raw)]
    except ValueError:
        log.warning("Битый payload сообщения a_checks: %r", str(raw)[:120])
        return []
