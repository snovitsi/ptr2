"""Логирование пайплайна: файл с статусами и ошибками + вывод в консоль.

Каждый скрипт/воркер пишет свой файл (по умолчанию <LOG_DIR>/<имя_этапа>.log,
например logs/fetch.log, logs/ptr_worker-01.log), поэтому логи нескольких
python-хостов не конфликтуют при записи (файлы лежат локально на каждом хосте).

Формат строки:
    2026-11-01 12:00:00,123 INFO  [host] [ptr2.ptr] [w1] сообщение
    ... traceback при log.exception()

Уровни (соглашение по всему проекту):
    INFO     — статусы этапов (старт/финиш, сколько обработано, конфигурация);
    WARNING  — штатные отклонения (таймауты DNS, ретраи, пропуск задач);
    ERROR    — ошибки задач (упавший шард, ошибка записи в PG/Redis);
    CRITICAL — фатальные сбои процесса.

Настройки (.env):
    LOG_LEVEL=INFO            уровень консоли и файлов
    LOG_TO_FILE=true          писать ли в файл (по умолчанию true)
    LOG_DIR=./logs            каталог логов (создаётся автоматически)
    LOG_FILE=                 явное имя файла (переопределяет этап; для 1 хоста)
    LOG_MAX_BYTES=10485760    ротация: максимальный размер файла (10 MiB)
    LOG_BACKUP_COUNT=5        ротация: число архивных файлов (.1 .2 ...)
    LOG_CONSOLE=true          дублировать в stderr
"""

from __future__ import annotations

import atexit
import logging
import os
import socket
import sys
from logging.handlers import RotatingFileHandler

_configured = False
_file_handlers: list[RotatingFileHandler] = []

DEFAULT_NAME = "ptr2"


def _env(key: str, default: str) -> str:
    v = os.environ.get(key)
    if v is None or v.strip() == "":
        return default
    return v.strip()


def _env_bool(key: str, default: bool) -> bool:
    v = _env(key, "yes" if default else "no").lower()
    return v in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


_HOSTNAME = socket.gethostname().split(".")[0]


class HostFieldFilter(logging.Filter):
    """Добавляет поле record.host для формата FMT — полезно в распределённом
    режиме (N хостов пишут логи с меткой своего имени).

    ВНИМАНИЕ: фильтр навешивается на ОБРАБОТЧИКИ, а не на логгер. Фильтры
    логгеров применяются только к записям, созданным непосредственно этим
    логгером, и НЕ наследуются дочерними логгерами: сообщение от
    `ptr2.fetch`/`ptr2.schema` прошло бы мимо фильтра корневого логгера, и
    формат %(host)s упал бы с KeyError при первой же записи.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "host"):
            record.host = _HOSTNAME  # type: ignore[attr-defined]
        return True


class WorkerFieldFilter(logging.Filter):
    """Добавляет поле record.worker ("-" если тег не задан env WORKER_TAG).

    Нужен, чтобы в общем логе (pipeline.log/консоль) строки нескольких
    процессов-воркеров с одинаковой меткой [w1] различались: префикс
    формата печатает [%(worker)s]. Как и HostFieldFilter, вешается на
    обработчики — только так он видит записи дочерних логгеров.
    """

    def __init__(self, tag: str | None = None):
        super().__init__()
        self.tag = tag if tag is not None else _env("WORKER_TAG", "")

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "worker"):
            record.worker = self.tag.strip("-") or "-"  # type: ignore[attr-defined]
        return True


FMT = "%(asctime)s %(levelname)-8s [%(host)s] [%(name)s] [%(worker)s] %(message)s"
DATEFMT = "%Y-%m-%d %H:%M:%S"


def resolve_log_path(name: str | None, cfg_data_dir: str | None = None) -> str:
    """Путь файла лога: LOG_FILE > LOG_DIR/<name>.log > <data_dir>/../logs/<name>.log."""
    explicit = _env("LOG_FILE", "")
    if explicit:
        path = os.path.expanduser(explicit)
    else:
        log_dir = _env("LOG_DIR", "")
        if not log_dir:
            base = cfg_data_dir or "./data"
            log_dir = os.path.join(os.path.dirname(os.path.abspath(base)), "logs")
        safe = (name or DEFAULT_NAME).replace("/", "_")
        path = os.path.join(log_dir, f"{safe}.log")
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    return path


def worker_tag() -> str:
    """Суффикс имени процесса/файла лога из WORKER_TAG (его задаёт run_pipeline.sh
    для параллельных воркеров одного этапа: "-01", "-02", ...)."""
    return os.environ.get("WORKER_TAG", "")


def setup_logging(level: str = "INFO", name: str = DEFAULT_NAME,
                  to_file: bool | None = None, tag: str = "") -> logging.Logger:
    """Настраивает корневой логгер ptr2: консоль + файл с ротацией.

    Идемпотентна: повторные вызовы не добавляют дублирующих обработчиков.

    :param name: имя этапа для имени файла лога (fetch/shard/ptr_worker/a/web/owa/report)
    :param tag:  суффикс файла, например f"-{worker_no}" или f"-{hostname}-{pid}",
                 чтобы несколько процессов на одном хосте писали в разные файлы
                 (или в один общий — если tag одинаковый у группы воркеров).
    :return: логгер `ptr2.<name>` для непосредственной записи.
    """
    # НЕ сбрасываем уже настроенные обработчики: несколько вызовов (разные этапы
    # в одном процессе) не должны лишать консольный обработчик фильтров host/worker,
    # иначе %(host)s/%(worker)s выпадают с KeyError для записей после 1-го вызова.
    global _configured

    if to_file is None:
        to_file = _env_bool("LOG_TO_FILE", True)

    root = logging.getLogger(DEFAULT_NAME)
    lvl = getattr(logging, str(level).upper(), logging.INFO)
    root.setLevel(lvl)
    formatter = logging.Formatter(FMT, datefmt=DATEFMT)
    host_filter = HostFieldFilter()
    worker_filter = WorkerFieldFilter()

    if not _configured:
        for h in list(root.handlers):
            root.removeHandler(h)
            try:
                h.close()
            except Exception:  # noqa: BLE001
                pass

        if _env_bool("LOG_CONSOLE", True):
            sh = logging.StreamHandler(sys.stderr)
            sh.setFormatter(formatter)
            sh.addFilter(host_filter)     # именно на handler: см. HostFieldFilter
            sh.addFilter(worker_filter)
            root.addHandler(sh)

        if to_file:
            fname = name if not tag else f"{name}{tag}"
            path = resolve_log_path(fname)
            fh = RotatingFileHandler(
                path,
                maxBytes=_env_int("LOG_MAX_BYTES", 10 * 1024 * 1024),
                backupCount=_env_int("LOG_BACKUP_COUNT", 5),
                encoding="utf-8",
            )
            fh.setFormatter(formatter)
            fh.addFilter(host_filter)
            fh.addFilter(worker_filter)
            root.addHandler(fh)
            _file_handlers.append(fh)
            atexit.register(_flush_close_all)

        # молчим болтливые библиотеки, но их ошибки всё равно проходят через ERROR
        for noisy in ("asyncio", "urllib3", "aiohttp.access", "dns"):
            logging.getLogger(noisy).setLevel(max(lvl, logging.WARNING))

        _configured = True
    else:
        # повторный вызов: обновляем уровни существующих обработчиков
        for h in root.handlers:
            h.setLevel(lvl)
        root.setLevel(lvl)

    logger = logging.getLogger(f"{DEFAULT_NAME}.{name}") if name != DEFAULT_NAME \
        else logging.getLogger(DEFAULT_NAME)
    logger.debug("логgers настроен: level=%s handlers=%d", lvl, len(root.handlers))
    return logger


def _flush_close_all() -> None:
    for h in _file_handlers:
        try:
            h.flush()
            h.close()
        except Exception:  # noqa: BLE001
            pass


def log_stage(logger: logging.Logger, stage: str, status: str, **kv) -> None:
    """Стандартизированная строка статуса этапа: key=value для grep/парсинга."""
    extra = " ".join(f"{k}={v}" for k, v in kv.items())
    logger.info("STAGE %s STATUS %s %s", stage, status, extra)


def get_log_paths() -> list[str]:
    """Пути активных файловых логов процесса (для вывода в конце run_pipeline.sh)."""
    out = []
    for h in _file_handlers:
        try:
            out.append(h.baseFilename)
        except Exception:  # noqa: BLE001
            pass
    return out


# ---------------------------------------------------------------------------
# Общий лог пайплайна: статусы этапов и ошибки (одна строка на событие,
# пишется атомарно — безопасно для нескольких процессов/хостов в append-режиме)
# ---------------------------------------------------------------------------

def pipeline_log_path() -> str:
    log_dir = _env("LOG_DIR", "")
    if not log_dir:
        base = _env("DATA_DIR", "./data")
        log_dir = os.path.join(os.path.dirname(os.path.abspath(base)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "pipeline.log")


def stage_event(stage: str, status: str, host: str | None = None, **kv) -> None:
    """Строка вида: 2026-11-01T12:00:00Z host=py1 stage=ptr status=START workers=4

    status: START | DONE | FAIL | INFO
    """
    import datetime as _dt

    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    h = host or _HOSTNAME
    parts = [f"{ts}", f"host={h}", f"stage={stage}", f"status={status}"]
    parts += [f"{k}={v}" for k, v in kv.items()]
    line = " ".join(parts) + "\n"
    try:
        with open(pipeline_log_path(), "a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError:
        # лог-файл не должен ронять рабочий процесс
        logging.getLogger(DEFAULT_NAME).warning("не удалось писать в pipeline.log: %s", line.strip())


def stage_start(stage: str, **kv) -> None:
    stage_event(stage, "START", **kv)


def stage_done(stage: str, **kv) -> None:
    stage_event(stage, "DONE", **kv)


def stage_fail(stage: str, error: str = "", **kv) -> None:
    stage_event(stage, "FAIL", error=error.replace("\n", " ")[:500], **kv)
