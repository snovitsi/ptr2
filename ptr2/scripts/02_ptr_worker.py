#!/usr/bin/env python3
"""Шаг 2 (воркер): опрос PTR DNS-записей для всех адресов шардов из Redis.

Распределённый режим по умолчанию: задачи берутся из Redis Stream
ptr2:stream:ptr_shards через consumer group — скрипт можно запускать
параллельно на НЕОГРАНИЧЕННОМ числе python-хостов и внутри хоста в несколько
процессов; Redis сам распределяет сообщения между потребителями группы.
Если хост упал, его неподтверждённые (зависшие) задачи через CLAIM_IDLE_MS
перехватываются другими воркерами (XAUTOCLAIM).

Анти-BAN (см. ptr2/stratify.py):
  * очередь задач уже построена round-robin по /24-семьям (01_select_and_shard),
    поэтому соседние задачи — из разных reverse-зон;
  * SpreadGuard через Redis дополнительно ограничивает число ОДНОВРЕМЕННЫХ
    активных семей (SPREAD_MAX_CONCURRENT на весь кластер) и не даёт двум
    воркерам одновременно долбить одну и ту же /24;
  * адреса внутри шарда опрашиваются в детерминированном перемешанном порядке
    (PTR_SHUFFLE_WITHIN_SHARD=true), а не подряд.

Найденные пары (ip, hostname) пишутся в stream ptr2:stream:ptr_results,
откуда их забирает 03_load_ptr.py и складывает в PostgreSQL.

Задача в очереди: {"shard_id":N,"range_id":M,"cc":"DE","start":"1.2.3.0",
                    "end":"1.2.3.255","family_key":"1.2.3.0/24"}

Использование:
    # на каждом python-хосте (DATABASE_URL/REDIS_URL указывают на общие серверы):
    python -m ptr2.scripts.02_ptr_worker                 # крутится до Ctrl-C
    python -m ptr2.scripts.02_ptr_worker --once          # вычитать текущее и выйти
    python -m ptr2.scripts.02_ptr_worker --workers 4     # 4 параллельных цикла в процессе
    USE_STREAMS=false python -m ptr2.scripts.02_ptr_worker   # старая списковая очередь
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ptr2.logsetup import setup_logging, stage_event  # noqa: E402
from ptr2.config import get_config, stream_consumer, stream_producer  # noqa: E402
from ptr2.dnslib import DnsClient, iter_ips  # noqa: E402
from ptr2.redisq import SimpleQueue, get_redis  # noqa: E402
from ptr2.stratify import SpreadGuard, family_of, shuffled_indices  # noqa: E402

log = logging.getLogger("ptr2.ptr")

STOP = asyncio.Event()


async def process_shard(task: dict, dns: DnsClient, emit, cfg) -> int:
    """Опрашивает все адреса шарда; emit(list[dict]) вызывается для найденных записей."""
    start, end = str(task["start"]), str(task["end"])
    shard_id = task.get("shard_id")
    if isinstance(shard_id, str):
        shard_id = int(shard_id)
    ips = list(iter_ips(start, end, cfg.skip_network_and_broadcast))

    # Анти-BAN: перемешиваем порядок обхода внутри шарда (детерминированный
    # шифт по family_key — разные /24 обходятся в разном порядке).
    fam = str(task.get("family_key") or family_of(start, end, cfg.family_bits))
    if cfg.ptr_shuffle_within_shard and len(ips) > 1:
        key = f"{fam}:{shard_id}"
        ips = [ips[i] for i in shuffled_indices(len(ips), key)]

    results: list[dict] = []

    async def one(ip: str):
        hosts = await dns.query_ptr(ip)
        for h in hosts:
            results.append({"ip": ip, "hostname": h, "shard_id": shard_id})

    chunk = max(cfg.ptr_concurrency * 2, 32)
    t_start = time.monotonic()
    last_beat = t_start
    for i in range(0, len(ips), chunk):
        if STOP.is_set():
            break
        now = time.monotonic()
        if now - last_beat >= 15:  # heartbeat: видно, что обход идёт, а не завис
            log.info("... shard %s: опрос %d/%d адресов (%.0fс)",
                     shard_id, min(i, len(ips)), len(ips), now - t_start)
            last_beat = now
        batch = ips[i : i + chunk]
        await asyncio.gather(*(one(ip) for ip in batch))
    if results:
        emit(results)
    return len(results)


def _mark_shard(cfg, shard_id: int | None, status: str) -> None:
    if shard_id is None:
        return
    import psycopg

    with psycopg.connect(cfg.dsn(), autocommit=True) as conn:
        conn.execute(
            "UPDATE shards SET status=%s, finished_at=now() WHERE id=%s", (status, int(shard_id))
        )


# --------------------------------------------------------------------------
# Анти-BAN: ожидание слота семьи (SpreadGuard) без блокировки event loop
# --------------------------------------------------------------------------

async def acquire_family_async(guard: SpreadGuard, family: str, stop: asyncio.Event,
                               worker_no: int, once: bool) -> bool:
    """Занимает слот семьи; ждёт с backoff, пока семья/лимит не освободятся.

    Возвращает False, если за время ожидания очередь опустела и включён --once
    или получен сигнал остановки (задачу в этом случае caller должен вернуть).
    """
    waited = 0.0
    while not stop.is_set():
        ok = await asyncio.to_thread(guard.try_acquire, family)
        if ok:
            return True
        await asyncio.sleep(min(2.0 + (waited % 3.0), 5.0))
        waited += 2.0
        if waited >= 15.0:
            log.debug("[w%d] жду освобождения семьи %s (%.0fс)", worker_no, family, waited)
    return False


# --------------------------------------------------------------------------
# Режим Redis Streams (распределённый, много хостов)
# --------------------------------------------------------------------------

async def stream_loop(worker_no: int, cfg, dns: DnsClient, in_c, out_p, queue_only: bool,
                      once: bool, max_shards: int | None, guard: SpreadGuard | None = None):
    processed = 0
    failed = 0
    idle_rounds = 0
    t0 = time.time()
    last_beat = t0
    held: list[str] = []

    def _heartbeat(pend_now: int) -> None:
        nonlocal last_beat
        now = time.time()
        if now - last_beat >= 30:
            log.info("[w%d] жив, но жду работу: обработано %d, PEL группы=%d, %.0fс",
                     worker_no, processed, pend_now, now - t0)
            last_beat = now

    try:
        while not STOP.is_set():
            if max_shards and processed >= max_shards:
                break
            # 1) сначала перехватываем зависшие задачи (после падения других хостов).
            #    min_idle берём из аренды SpreadGuard: задача «зависла» честно, если
            #    её воркер мёртв (слот семьи истёк) или превысил CLAIM_IDLE_MS.
            claim_idle = max(cfg.claim_idle_ms,
                             (cfg.spread_lease_sec + 15) * 1000) if guard is not None \
                else cfg.claim_idle_ms
            # НЕ перехватываем чужие задачи на старте: живой воркер-владелец
            # может держать шард в работе дольше CLAIM_IDLE_MS (медленный DNS),
            # а XAUTOCLAIM отдаёт сообщения ВЛАДЕЛЬЦУ — двойная работа и лишний
            # BAN-риск. Перехват нужен только при простое этого цикла и для
            # задач после падения хоста (их больше никто не читает).
            stale = in_c.claim_stale(min_idle_ms=claim_idle, count=10) \
                if idle_rounds else []
            msgs = stale or in_c.read(count=10, block_ms=2000)
            if not msgs:
                if once:
                    # depth()==lag недоступен в Redis <7.2 — ориентируемся на PEL:
                    # свои незакрытые задачи есть -> работаем; пусто и 3 раунда тишины
                    # (6 с) -> других воркеров дождёмся через claim_stale ниже.
                    pend = in_c.pending_total()
                    if pend > 0:
                        idle_rounds = 0
                        _heartbeat(pend)
                        continue
                    idle_rounds += 1
                    if idle_rounds > 3:
                        stale = in_c.claim_stale(min_idle_ms=claim_idle, count=10)
                        if stale:
                            idle_rounds = 0
                            msgs = stale
                        else:
                            break
                    continue
                continue
            idle_rounds = 0
            for msg_id, task in msgs:
                fam = str(task.get("family_key")
                          or family_of(str(task["start"]), str(task["end"]), cfg.family_bits))
                # анти-BAN: не более SPREAD_MAX_CONCURRENT семей одновременно,
                # и одна семья не обрабатывается двумя воркерами сразу
                if guard is not None:
                    got = await acquire_family_async(guard, fam, STOP, worker_no, once)
                    if not got:
                        # сигнал остановки: вернуть в PEL со СБРОСOM idle, чтобы
                        # живой воркер перехватил задачу сразу (не через 120с)
                        in_c.release_unprocessed(msg_id)
                        continue
                    held.append(fam)
                try:
                    n = await process_shard(task, dns, out_p.send_batch, cfg)
                    log.info("[w%d] shard %s (%s): найдено %d хостнеймов", worker_no,
                             task.get("shard_id"), fam, n)
                    if STOP.is_set():
                        # прерваны Ctrl-C/_SIGTERM в середине обхода: результат
                        # неполный -> НЕ ack'ем; вернуть в PEL со сбросом idle,
                        # живой воркер дообработает семью целиком
                        in_c.release_unprocessed(msg_id)
                        continue
                    if not queue_only:
                        _mark_shard(cfg, task.get("shard_id"), "done")
                    in_c.ack(msg_id)
                    processed += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    log.exception("[w%d] shard %s упал: %s", worker_no, task.get("shard_id"), exc)
                    stage_event("ptr_worker", "INFO", shards_failed=failed,
                                last_shard=task.get("shard_id"), error=repr(exc)[:300])
                    # ack'нем, чтобы не зацикливать битую задачу; статус — error
                    try:
                        _mark_shard(cfg, task.get("shard_id"), "error")
                    except Exception:
                        pass
                    in_c.ack(msg_id)
                    await asyncio.sleep(1)
                finally:
                    if guard is not None and held and held[-1] == fam:
                        guard.release(held.pop())
            if STOP.is_set():
                break
    finally:
        if guard is not None:
            for fam in reversed(held):
                guard.release(fam)
    if STOP.is_set():
        log.warning("[w%d] остановлен по сигналу (SIGINT/SIGTERM): обработано шардов %d за %.1fс",
                    worker_no, processed, time.time() - t0)
    log.info("[w%d] завершил работу: обработано шардов %d за %.1fс",
             worker_no, processed, time.time() - t0)
    stage_event("ptr_worker", "INFO", worker=worker_no, shards_done=processed,
                shards_failed=failed, seconds=round(time.time() - t0, 1))
    return processed


# --------------------------------------------------------------------------
# Режим старой списковой очереди (USE_STREAMS=false)
# --------------------------------------------------------------------------

async def list_loop(worker_no: int, cfg, dns: DnsClient, in_q: SimpleQueue,
                    out_q: SimpleQueue, queue_only: bool, once: bool, max_shards: int | None,
                    guard: SpreadGuard | None = None):
    processed = 0
    failed = 0
    t0 = time.time()
    held: list[str] = []
    try:
        while not STOP.is_set():
            if max_shards and processed >= max_shards:
                break
            item = in_q.pop(timeout=2)
            if item is None:
                if once:
                    break
                continue
            raw, task = item
            fam = str(task.get("family_key")
                      or family_of(str(task["start"]), str(task["end"]), cfg.family_bits))
            if guard is not None:
                got = await acquire_family_async(guard, fam, STOP, worker_no, once)
                if not got:
                    in_q.requeue(raw)
                    break
                held.append(fam)
            try:
                n = await process_shard(task, dns, out_q.push_batch, cfg)
                log.info("[w%d] shard %s (%s): %d хостнеймов", worker_no,
                         task.get("shard_id"), fam, n)
                in_q.ack(raw)
                processed += 1
                if not queue_only:
                    _mark_shard(cfg, task["shard_id"], "done")
            except Exception as exc:  # noqa: BLE001
                failed += 1
                log.exception("[w%d] shard %s упал: %s", worker_no, task.get("shard_id"), exc)
                stage_event("ptr_worker", "INFO", shards_failed=failed,
                            last_shard=task.get("shard_id"), error=repr(exc)[:300])
                in_q.requeue(raw)
                try:
                    _mark_shard(cfg, task["shard_id"], "error")
                except Exception:
                    pass
                await asyncio.sleep(1)
            finally:
                if guard is not None and held and held[-1] == fam:
                    guard.release(held.pop())
    finally:
        if guard is not None:
            for fam in reversed(held):
                guard.release(fam)
    log.info("[w%d] завершил работу: обработано шардов %d за %.1fс",
             worker_no, processed, time.time() - t0)
    stage_event("ptr_worker", "INFO", worker=worker_no, shards_done=processed,
                shards_failed=failed, seconds=round(time.time() - t0, 1))


async def run(args) -> int:
    cfg = get_config()
    r = get_redis(cfg.redis_url)

    # Анти-BAN: глобальный (через Redis) лимит одновременно активных /24-семей.
    guard: SpreadGuard | None = None
    if cfg.spread_enabled and not args.no_spread:
        consumer_name = cfg.worker_id or f"{__import__('socket').gethostname()}:{os.getpid()}"
        guard = SpreadGuard(
            r, cfg.key_shard_spread,
            max_families=max(cfg.spread_max_concurrent, args.workers),
            lease_sec=cfg.spread_lease_sec,
            consumer=consumer_name,
        )
        log.info("SpreadGuard: максимум %d семей (/24) одновременно, аренда %dс",
                 guard.max_families, cfg.spread_lease_sec)

    use_streams = cfg.streams_enabled and not args.list_queue
    if use_streams:
        consumers = [stream_consumer(cfg.stream_ptr_shards, suffix=f"ptr{args.worker_id_tag}{i + 1}")
                     for i in range(args.workers)]
        for c in consumers:
            c.ensure_group()
        out_p = stream_producer(cfg.stream_ptr_results)
        if args.reclaim:
            total = 0
            for c in consumers:
                total += c.pending_total()
            log.info("Неподтверждённых задач в группе %s: %d (будут перехвачены при простое >%d мс)",
                     cfg.consumer_group, total, cfg.claim_idle_ms)
    else:
        in_q = SimpleQueue(r, cfg.queue_shards_ptr)
        out_q = SimpleQueue(r, cfg.queue_ptr_results)
        if args.reclaim:
            n = in_q.reclaim_stale()
            log.info("Возвращено зависших задач в очередь: %d", n)

    dns = DnsClient(
        nameservers=cfg.dns_nameservers or None,
        timeout=cfg.dns_timeout,
        lifetime=cfg.dns_lifetime,
        concurrency=max(cfg.ptr_concurrency // args.workers, 10) if args.workers > 1 else cfg.ptr_concurrency,
        rate_limit=cfg.ptr_rate_limit / max(args.workers, 1),
    )

    loop = asyncio.get_running_loop()

    def _on_stop(sig_name: str) -> None:
        if not STOP.is_set():
            log.warning("Получен %s — завершаю воркеры (прерву текущие чанки DNS-опроса)...",
                        sig_name)
        STOP.set()

    for sig, name in ((signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")):
        try:
            loop.add_signal_handler(sig, _on_stop, name)
        except NotImplementedError:
            pass

    if use_streams:
        # НЕ выходим по «пустому» depth(): lag группы недоступен в Redis <7.2,
        # а другие воркеры могли только взять задачи в работу (PEL>0).
        # stream_loop сам завершится по PEL+claim_stale или на --max-shards.
        tasks = [
            asyncio.create_task(
                stream_loop(i + 1, cfg, dns, consumers[i % len(consumers)], out_p,
                            args.queue_only, args.once, args.max_shards, guard)
            )
            for i in range(args.workers)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Готово. ptr_results сообщений в stream: %d", r.xlen(cfg.stream_ptr_results))
    else:
        if args.once and in_q.depth() == 0:
            log.info("Очередь пуста (--once), выходим")
            return 0
        tasks = [
            asyncio.create_task(
                list_loop(i + 1, cfg, dns, in_q, out_q, args.queue_only,
                          once=args.once, max_shards=args.max_shards, guard=guard)
            )
            for i in range(args.workers)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
        log.info("Готово. В очереди ptr_results задач: %d, зависших: %d",
                 out_q.depth(), in_q.processing_depth())
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workers", type=int, default=1, help="число параллельных циклов внутри процесса")
    ap.add_argument("--once", action="store_true", help="обработать всё, что есть, и выйти")
    ap.add_argument("--reclaim", action="store_true", help="лог зависимых задач / возврат в готовящиеся")
    ap.add_argument("--queue-only", action="store_true", help="не обновлять статус шардов в PG")
    ap.add_argument("--list-queue", action="store_true", help="принудительно старая списковая очередь")
    ap.add_argument("--worker-id", default=None, help="имя потребителя (по умолчанию hostname:pid)")
    ap.add_argument("--max-shards", type=int, default=None, help="лимит шардов на процесс (для теста)")
    ap.add_argument("--no-spread", action="store_true",
                    help="отключить глобальный лимит одновременных /24-семей (анти-BAN)")
    args = ap.parse_args()
    # тег экземпляра из run_pipeline.sh (WORKER_TAG="-01") — уникальность имён
    # потребителей в группе при нескольких воркерах на одном хосте
    args.worker_id_tag = os.environ.get("WORKER_TAG", "")

    if args.worker_id:
        os.environ["WORKER_ID"] = args.worker_id

    cfg = get_config()
    # каждый воркер пишет в отдельный файл logs/ptr_worker-<NN>.log
    setup_logging(cfg.log_level, name="ptr_worker",
                  tag=os.environ.get("WORKER_TAG", ""))
    # строки помечаются тегом процесса через формат [%(worker)s] (см. logsetup)
    log.info("Конфигурация: redis=%s pg=%s streams=%s consumer=%s pid=%d",
             cfg.redis_url.split("@")[-1], cfg.database_url.split("@")[-1],
             cfg.streams_enabled and not args.list_queue,
             os.environ.get("WORKER_ID") or "hostname:pid", os.getpid())
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
