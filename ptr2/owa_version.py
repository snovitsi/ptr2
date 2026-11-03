"""Определение версии Microsoft Exchange / OWA по HTTP-ответам.

Источники версии (по убыванию надёжности):
  1. заголовок X-Owa-Version          — CU-уровень фронтенда (Exchange 2013+);
  2. теле: var g_feOwaVersion / feOwaVersion — версия OWA в логине;
  3. meta-tag msexccver               — версия сервера (2010/2007);
  4. cookie ClientInfo                — build + CU (Exchange 2010);
  5. теле "OWA version 14.x ..."      — строка из logon.aspx;
  6. заголовок X-MSErr-* / X-Exchangewebmoduleversion и т.п.

Сырые номера сборок нормализуются к виду xx.xx.xxxx.xxx и сопоставляются
с таблицей известных релизов Exchange (build -> версия продукта).
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Справочник: старшие 3 компонента сборки -> (версия продукта, описание)
# Используется, когда CU-номер неизвестен или не найден в таблице ниже.
# ---------------------------------------------------------------------------
BUILD_PREFIX_MAP: list[tuple[str, str]] = [
    # Exchange 2019
    ("15.02.0", "Exchange 2019"),
    ("15.2.", "Exchange 2019"),
    # Exchange 2016
    ("15.01.0", "Exchange 2016"),
    ("15.01.2", "Exchange 2016"),
    ("15.1.", "Exchange 2016"),
    # Exchange 2013
    ("15.00.0", "Exchange 2013"),
    ("15.00.1", "Exchange 2013"),
    ("15.0.", "Exchange 2013"),
    # Exchange 2010
    ("14.00.0", "Exchange 2010"),
    ("14.01.0", "Exchange 2010 SP1"),
    ("14.02.0", "Exchange 2010 SP2 / RU rollup"),
    ("14.03.0", "Exchange 2010 SP3"),
    ("8.3.264", "Exchange 2007 SP3"),
    ("8.1.", "Exchange 2007 SP1"),
    ("8.0.", "Exchange 2007"),
    ("6.5.", "Exchange 2003"),
]

# Точные сборки, которые часто встречаются в дикой природе (CU/GUR)
EXACT_BUILDS: dict[str, str] = {
    # Exchange 2019
    "15.02.0330.005": "Exchange 2019 RTM",
    "15.02.0410.003": "Exchange 2019 CU1",
    "15.02.0478.005": "Exchange 2019 CU2",
    "15.02.0513.005": "Exchange 2019 CU3",
    "15.02.0595.003": "Exchange 2019 CU4",
    "15.02.0659.005": "Exchange 2019 CU5",
    "15.02.0720.019": "Exchange 2019 CU6",
    "15.02.0750.006": "Exchange 2019 CU7",
    "15.02.0792.010": "Exchange 2019 CU8",
    "15.02.0858.005": "Exchange 2019 CU9",
    "15.02.0925.005": "Exchange 2019 CU10",
    "15.02.0986.005": "Exchange 2019 CU11",
    "15.02.1118.007": "Exchange 2019 CU12",
    "15.02.1262.004": "Exchange 2019 CU13",
    "15.02.1544.011": "Exchange 2019 CU14",
    "15.02.1544.013": "Exchange 2019 CU14",
    "15.02.1748.012": "Exchange 2019 CU15",
    # Exchange 2016
    "15.01.0210.002": "Exchange 2016 RTM",
    "15.01.0358.001": "Exchange 2016 CU1",
    "15.01.0469.010": "Exchange 2016 CU2",
    "15.01.0544.010": "Exchange 2016 CU3",
    "15.01.0678.005": "Exchange 2016 CU5",
    "15.01.0845.033": "Exchange 2016 CU6",
    "15.01.0919.010": "Exchange 2016 CU7",
    "15.01.1015.003": "Exchange 2016 CU8",
    "15.01.1044.014": "Exchange 2016 CU9",
    "15.01.1261.038": "Exchange 2016 CU10",
    "15.01.1415.002": "Exchange 2016 CU11",
    "15.01.1466.003": "Exchange 2016 CU12",
    "15.01.1591.010": "Exchange 2016 CU13",
    "15.01.1747.007": "Exchange 2016 CU14",
    "15.01.1913.005": "Exchange 2016 CU15",
    "15.01.2106.002": "Exchange 2016 CU16",
    "15.01.2242.012": "Exchange 2016 CU17",
    "15.01.2308.015": "Exchange 2016 CU18",
    "15.01.2376.017": "Exchange 2016 CU19",
    "15.01.2507.006": "Exchange 2016 CU20",
    "15.01.2507.017": "Exchange 2016 CU21",
    "15.01.2507.021": "Exchange 2016 CU22",
    "15.01.2507.027": "Exchange 2016 CU23",
    "15.01.2507.035": "Exchange 2016 CU23",
    "15.01.2507.044": "Exchange 2016 CU23",
    "15.01.2507.057": "Exchange 2016 CU23 (latest SU)",
    "15.01.2507.061": "Exchange 2016 CU23 (latest SU)",
    "15.01.2507.068": "Exchange 2016 CU23 (latest SU)",
    "15.01.2507.076": "Exchange 2016 CU23 (latest SU)",
    # Exchange 2013
    "15.00.0457.005": "Exchange 2013 CU6",
    "15.00.0516.025": "Exchange 2013 CU7",
    "15.00.0620.009": "Exchange 2013 CU9",
    "15.00.0721.002": "Exchange 2013 CU10",
    "15.00.0845.033": "Exchange 2013 CU11",
    "15.00.1104.005": "Exchange 2013 CU15",
    "15.00.1236.003": "Exchange 2013 CU16",
    "15.00.1320.004": "Exchange 2013 CU17",
    "15.00.1365.000": "Exchange 2013 CU18",
    "15.00.1367.003": "Exchange 2013 CU19",
    # Exchange 2010
    "14.00.0639.001": "Exchange 2010 RTM",
    "14.01.0250.000": "Exchange 2010 SP1",
    "14.01.0438.001": "Exchange 2010 SP1 RU3",
    "14.02.0030.002": "Exchange 2010 SP2",
    "14.03.0123.003": "Exchange 2010 SP3",
    "14.03.0361.001": "Exchange 2010 SP3 (RU)",
}

# Major.minor -> продукт (для сокращённых номеров вида "15.20" из X-Owa-Version).
# ВНИМАНИЕ: у Exchange 2016 заголовок X-Owa-Version приходит как "15.20",
# а номер сборки в теле — как "15.01.xxxx"; обе ветки ведут в Exchange 2016.
MAJOR_MINOR_MAP = {
    "15.2": "Exchange 2019",
    "15.21": "Exchange 2019",
    "15.22": "Exchange 2019",
    "15.23": "Exchange 2019",
    "15.24": "Exchange 2019",
    "15.25": "Exchange 2019",
    "15.1": "Exchange 2016",
    "15.01": "Exchange 2016",
    "15.20": "Exchange 2016",   # X-Owa-Version для 2016 приходит как 15.20.xx
    "15.0": "Exchange 2013",
    "15.00": "Exchange 2013",
    "14.0": "Exchange 2010",
    "14.1": "Exchange 2010 SP1",
    "14.2": "Exchange 2010 SP2",
    "14.3": "Exchange 2010 SP3",
    "8.3": "Exchange 2007 SP3",
    "8.2": "Exchange 2007 SP2",
    "8.1": "Exchange 2007 SP1",
    "8.0": "Exchange 2007",
    "6.5": "Exchange 2003",
}


def normalize_build(raw: str) -> str | None:
    """Приводит номер сборки к виду XX.YY.ZZZZ.SSS (4 группы, с ведущими нулями).

    Принимает "15.1.2507.17", "15.01.2507.017", "15.20", "15.20.659.013".
    Возвращает None, если в строке нет последовательности чисел, разделённых
    точками (например "Exchange Server 2010 SP3" — это текст, а не сборка).
    """
    if not raw:
        return None
    # ищем именно dotted-последовательность: 2..4 числа через точку
    m = re.search(r"\d+(?:\.\d+){1,3}", raw)
    if not m:
        return None
    nums = m.group(0).split(".")
    parts = [int(n) for n in nums[:4]]
    while len(parts) < 4:
        parts.append(0)
    return f"{parts[0]:02d}.{parts[1]:02d}.{parts[2]:04d}.{parts[3]:03d}"


def is_partial_build(build_norm: str) -> bool:
    """Сборка неполная (только major.minor из X-Owa-Version вида '15.20')."""
    return build_norm.endswith(".0000.000")


def product_from_major_minor(major: int, minor: int) -> str | None:
    key = f"{major}.{minor}"
    if key in MAJOR_MINOR_MAP:
        return MAJOR_MINOR_MAP[key]
    # сворачиваем 15.01 -> 15.1, 15.02 -> 15.2
    key2 = f"{major}.{minor // 10 if minor >= 10 else minor}"
    return MAJOR_MINOR_MAP.get(key2)


def describe_build(build_norm: str) -> tuple[str | None, str | None]:
    """По нормализованной сборке возвращает (exchange_version, build_number)."""
    if not build_norm:
        return None, None
    exact = EXACT_BUILDS.get(build_norm)
    if exact:
        return exact, build_norm
    major, minor = int(build_norm[:2]), int(build_norm[3:5])
    prod = product_from_major_minor(major, minor)
    if prod:
        if is_partial_build(build_norm):
            return prod, None          # известна только линейка, сборки нет
        return f"{prod} (build {build_norm})", build_norm
    # префиксный поиск
    for pref, name in BUILD_PREFIX_MAP:
        if build_norm.startswith(pref):
            if is_partial_build(build_norm):
                return name, None
            return f"{name} (build {build_norm})", build_norm
    if is_partial_build(build_norm):
        return f"Unknown Exchange ({build_norm[:5]})", None
    return f"Unknown Exchange (build {build_norm})", build_norm


# --- регэкспы для поиска версий в HTTP-ответах -----------------------------

RE_FEOWA_BODY = re.compile(
    r"(?:var\s+g_feOwaVersion|feOwaVersion)\s*=\s*[\"']?([\d.]+)", re.I)
RE_MSEXCCVER_META = re.compile(
    r'name=["\']msexccver["\']\s+content=["\']([^"\']+)["\']', re.I)
RE_MSEXCCVER_ALT = re.compile(
    r'content=["\']([^"\']+)["\']\s+name=["\']msexccver["\']', re.I)
RE_OWA_VERSION_TEXT = re.compile(r"OWA version\s+([\d.]+)", re.I)
RE_CLIENTINFO_CU = re.compile(r"[|;]Cu/(?:%20|\+)?([\d.]+)", re.I)
RE_BUILD_IN_TEXT = re.compile(r"\b(\d{2})\.(\d{1,2})\.(\d{3,4})\.(\d{1,3})\b")


def extract_versions(headers: dict, body: str, cookies: dict | None = None) -> list[dict]:
    """Собирает все кандидаты версии из ответа. Возвращает список словарей:
    {"source": ..., "raw": ..., "build": normalized|None}.
    """
    hl = {k.lower(): v for k, v in (headers or {}).items()}
    found: list[dict] = []

    def add(source: str, raw: str | None):
        if not raw:
            return
        raw = raw.strip()
        if not raw:
            return
        norm = normalize_build(raw) if any(c.isdigit() for c in raw) else None
        found.append({"source": source, "raw": raw[:120], "build": norm})

    # 1) X-Owa-Version: "15.20" / "15.1.2507.17:ServerWideOffline"
    add("header:x-owa-version", hl.get("x-owa-version"))
    # 2) X-Commondiagnostics / прочие exchange-заголовки с номером
    add("header:x-exchangewebmoduleversion", hl.get("x-exchangewebmoduleversion"))
    # 3) X-FEServer косвенно не даёт версию, но X-OWA-Canary нет; пропуск.

    low = body or ""
    # 4) feOwaVersion в JS
    m = RE_FEOWA_BODY.search(low)
    if m:
        add("body:feOwaVersion", m.group(1))
    # 5) meta msexccver
    m = RE_MSEXCCVER_META.search(low) or RE_MSEXCCVER_ALT.search(low)
    if m:
        add("body:meta-msexccver", m.group(1))
    # 6) "OWA version 14.3.466.005"
    m = RE_OWA_VERSION_TEXT.search(low)
    if m:
        add("body:owa-version-text", m.group(1))
    # 7) cookie ClientInfo=ExCDO=...|Cu/14.3.466.005
    ck = cookies or {}
    ci = ck.get("ClientInfo") or ck.get("clientinfo")
    if ci:
        m = RE_CLIENTINFO_CU.search(ci)
        if m:
            add("cookie:ClientInfo", m.group(1))

    # 8) fallback: первая 4-компонентная сборка в тексте
    if not any(f["build"] for f in found):
        m = RE_BUILD_IN_TEXT.search(low)
        if m and ("owa" in low.lower() or "exchange" in low.lower()):
            add("body:any-build-number", m.group(0))

    # дедупликация по (source, raw)
    uniq, seen = [], set()
    for f in found:
        k = (f["source"], f["raw"])
        if k not in seen:
            seen.add(k)
            uniq.append(f)
    return uniq


PREFERRED_ORDER = [
    "header:x-owa-version",
    "body:feOwaVersion",
    "cookie:ClientInfo",
    "body:meta-msexccver",
    "body:owa-version-text",
    "header:x-exchangewebmoduleversion",
    "body:any-build-number",
]


def pick_best_version(candidates: list[dict]) -> tuple[str | None, str | None, str | None]:
    """Возвращает (exchange_version, build_number, version_source) по лучшему кандидату.

    Приоритет: источник (PREFERRED_ORDER), затем полнота сборки —
    сокращённый номер из X-Owa-Version ("15.20") уступает место полной
    сборке из feOwaVersion/msexccver, если они описывают ту же линейку.
    """
    if not candidates:
        return None, None, None

    def rank(c: dict) -> tuple:
        src_i = PREFERRED_ORDER.index(c["source"]) if c["source"] in PREFERRED_ORDER else 99
        fullness = 0 if (c["build"] and not is_partial_build(c["build"])) else 1
        return (fullness, src_i)

    ordered = sorted(candidates, key=rank)
    best = ordered[0]
    ver, build = describe_build(best["build"]) if best["build"] else (None, None)
    if ver is None:
        # raw может быть уже человекочитаемым ("Exchange Server 2010")
        raw = best.get("raw") or ""
        if re.search(r"exchange|owa", raw, re.I):
            return raw, None, best["source"]
        return None, None, best["source"]
    return ver, build, best["source"]


def resolve_version(all_candidates: list[dict]) -> tuple[str | None, str | None, str | None]:
    """Агрегирует кандидатов с нескольких ответов и возвращает лучшую версию.

    Если заголовок X-Owa-Version дал только линейку ("15.20"), а другой
    источник — полную сборку той же масти, берём полную сборку и помечаем
    оба источника в version_source.
    """
    ver, build, src = pick_best_version(all_candidates)
    if build:
        full = next((c for c in all_candidates
                     if c["build"] and not is_partial_build(c["build"])
                     and c["build"] != build), None)
        # уточнение: x-owa-version(линейка) + полная сборка из тела/куки
        if (src and src.startswith("header:x-owa-version")
                and is_partial_build(build) and full):
            fver, fbuild = describe_build(full["build"])
            if fver:
                return fver, fbuild, f"{src}+{full['source']}"
    elif all_candidates:
        # лучшая версия без сборки, но где-то есть полная сборка — используем её
        full = next((c for c in all_candidates if c["build"]
                     and not is_partial_build(c["build"])), None)
        if full:
            fver, fbuild = describe_build(full["build"])
            if fver:
                return fver, fbuild, full["source"]
    return ver, build, src
