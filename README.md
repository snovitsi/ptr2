# ptr2 — пайплайн разведки RIPE-диапазонов (Python + Redis + PostgreSQL, без Docker)

Конвейер из независимых скриптов:

| Этап | Скрипт | Что делает |
|------|--------|------------|
| 0 | `ptr2/scripts/00_fetch_ripe.py` | Скачивает `https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-latest`, парсит и грузит диапазоны в PostgreSQL (`ripe_ranges`). Идемпотентен (UPSERT). |
| 1 | `ptr2/scripts/01_select_and_shard.py` | Отбирает диапазоны по критериям (**страна**, **размер**), режет их на **шарды по 256 адресов**, пишет в `shards` и кладёт задачи в Redis-очередь. |
| 2 | `ptr2/scripts/02_ptr_worker.py` | Воркер: опрашивает **PTR DNS-записи** всех адресов шардов из очереди (asyncio + dnspython, rate-limit). Результаты — в очередь `ptr_results`. Можно запускать несколько экземпляров параллельно. |
| 3 | `ptr2/scripts/03_load_ptr.py` | Вычитывает результаты PTR из Redis и пишет их в БД (`ptr_records`), одновременно ставит пары (hostname, ip) в очередь проверки A-записей. |
| 4 | `ptr2/scripts/04_check_a.py` | Проверяет для каждого найденного хостнейма наличие **прямой A-записи**; помечает `has_a_record` / `points_back` в `a_record_checks` и ставит хосты с A-записью в очередь веб-проверок. |
| 5 | `ptr2/scripts/05_check_web.py` | Проверяет **доступность веб-сервера** (http/https) по хостам с A-записью: статус, Server-заголовок, `<title>`, финальный URL → `web_checks`; доступные — в очередь OWA. |
| 6 | `ptr2/scripts/06_check_owa.py` | Ищет **Outlook Web Access**: пробы `/owa/`, `/owa/auth/logon.aspx`, `/autodiscover/autodiscover.xml`, анализ редиректов, OWA/Exchange-маркеров в заголовках и теле → `owa_checks` (is_owa, confidence, evidence). Плюс **определение версии Exchange**: X-Owa-Version, feOwaVersion в JS, meta msExCcVer, cookie ClientInfo → `exchange_version`, `build_number`, `version_source` (справочник сборок — `ptr2/owa_version.py`). |
| 7 | `ptr2/scripts/07_report.py` | Сводный отчёт по всем этапам + топ OWA-находок. |

Архитектура: PostgreSQL — источник истины и результатов; Redis — надёжные очереди задач.
Два транспорта очередей (выбираются флагом `USE_STREAMS`, по умолчанию — Streams):

- **Redis Streams + Consumer Groups** (`ptr2/redisq/stream_queue.py`) — распределённый
  режим: один и тот же воркер можно запускать на N хостах; at-least-once доставка,
  перехват зависших задач у упавших потребителей через `XAUTOCLAIM` (`CLAIM_IDLE_MS`).
- **Списковые очереди legacy** (`ptr2/redisq/list_queue.py`, BLMOVE/RPOPLPUSH,
  `ready` + `processing`) — режим одной машины; возврат зависших задач через `--reclaim`.

Оба реализуют одинаковый интерфейс (`push_batch/send_batch`, `pop/read`, `ack`), а выбор
транспорта централизован в хелперах `ptr2.config.producer()/consumer()` — код этапов
не содержит дублирующих веток. Этапы связаны очередями, каждый скрипт можно
перезапускать — запись идемпотентна (UPSERT).

### Структура проекта

```
run_pipeline.sh          # bash-оркестратор всех этапов (НЕ python!)
setup_env.sh             # venv + зависимости + PG/Redis + роль/БД + схема
requirements.txt         # redis, psycopg3, dnspython, aiohttp, tqdm
.env.example -> .env     # конфигурация (читается ptr2/config.py)
data/                    # кэш delegated-ripencc-latest
logs/                    # логи этапов + общий pipeline.log (LOG_DIR)
ptr2/
  config.py              # вся конфигурация из env/.env + хелперы producer/consumer
  db_schema.py           # DDL схемы PostgreSQL + мягкие миграции (python -m ptr2.db_schema)
  common.py              # CIDR-разбиение, транзакционная сессия БД
  logsetup.py            # логирование: консоль + файл этапа + pipeline.log
  dnslib.py              # async DNS (PTR/A/AAAA) c rate-limit
  stratify.py            # анти-BAN: семьи /24, round-robin, SpreadGuard, перемешивание
  owa_version.py         # справочник версий/сборок Exchange по OWA-маркерам
  apayload.py            # формат сообщений a_checks (пачки пар hostname,ip)
  redisq/                # очереди Redis: list_queue.py (legacy) + stream_queue.py (Streams)
  scripts/               # этапы 00..07 (запуск: python -m ptr2.scripts.<NN_name>)
```

---

## Требования (без Docker)

- Python 3.10+ (проверено на 3.12)
- PostgreSQL 14+ (нужны типы `inet`, `jsonb`)
- Redis 6+ (нужен `BLMOVE`, есть с Redis 6.2)
- Доступ к DNS-резолверу и выход в интернет (RIPE FTP/HTTPS, HTTP/HTTPS до целей)

Установка пакетов (Debian/Ubuntu):

```bash
sudo apt-get update
sudo apt-get install -y postgresql postgresql-client redis-server python3-venv python3-pip
```

## Быстрый старт

```bash
./setup_env.sh            # venv + зависимости + запуск PG/Redis + роль/БД + схема
source .venv/bin/activate
cp .env.example .env      # при необходимости поправьте критерии отбора/подключения
./run_pipeline.sh         # весь пайплайн от начала до отчёта
```

> **Важно:** `run_pipeline.sh` — это **Bash-скрипт**, а не Python. Запускать его
> нужно так: `./run_pipeline.sh` или `bash run_pipeline.sh`.
> Не запускайте его интерпретатором Python (`.venv/bin/python run_pipeline.sh`) —
> это вызовет `SyntaxError: unmatched ')'`, потому что Python не понимает
> bash-конструкции `case ... ;;`.

`setup_env.sh` можно запустить с `--no-apt`, если системные пакеты уже стоят.

### Ручная подготовка (альтернатива setup_env.sh)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

sudo service postgresql start          # или systemctl start postgresql
sudo service redis-server start        # или redis-server --daemonize yes

sudo -u postgres psql -c "CREATE ROLE ptr2 LOGIN PASSWORD 'ptr2';"
sudo -u postgres createdb -O ptr2 ptr2

cp .env.example .env
python -m ptr2.db_schema            # инициализация схемы
```

## Запуск по этапам

```bash
# 0. скачать RIPE и загрузить в БД (можно --limit 1000 для теста, --file локальный файл)
python -m ptr2.scripts.00_fetch_ripe

# 1. отбор по стране/размеру и шардинг по 256 адресов (критерии — из .env или CLI)
python -m ptr2.scripts.01_select_and_shard --countries DE,NL,FR --prefix-len 24 --max-ranges 10
#     dry-run без записи в БД/Redis:
python -m ptr2.scripts.01_select_and_shard --dry-run

# 2. PTR-обход (запустите несколько терминалов/процессов параллельно)
python -m ptr2.scripts.02_ptr_worker --workers 4 --once --reclaim

# 3. запись PTR-результатов в PostgreSQL
python -m ptr2.scripts.03_load_ptr --once

# 4. проверка A-записей
python -m ptr2.scripts.04_check_a --from-queue          # воркер по очереди
python -m ptr2.scripts.04_check_a --all --limit 5000    # или разовая выборка из БД

# 5. доступность веб-серверов
python -m ptr2.scripts.05_check_web --from-queue

# 6. поиск OWA
python -m ptr2.scripts.06_check_owa --from-queue

# 7. отчёт
python -m ptr2.scripts.07_report [--format csv]
```

### run_pipeline.sh

```bash
./run_pipeline.sh                                   # все этапы
./run_pipeline.sh --limit-ranges 5 --ptr-workers 2  # быстрый тест
SKIP=fetch ./run_pipeline.sh                        # пропустить скачивание RIPE
ONLY=report ./run_pipeline.sh                       # только отчёт
```

## Конфигурация (.env)

Полный commented-шаблон — в **`.env.example`** (`cp .env.example .env`). Файл `.env`
ищется в `$PTR2_ENV_FILE`, затем в текущем каталоге, затем в корне проекта;
реальные переменные окружения имеют приоритет над `.env`.

| Переменная | Описание |
|---|---|
| `RIPE_URL` | источник delegated-ripencc |
| `TARGET_COUNTRIES` | CC-коды для отбора, напр. `DE,NL,FR` (пусто = все) |
| `MIN_RANGE_SIZE` / `MAX_RANGE_SIZE` | фильтр по размеру диапазона в адресах (`256/256` = точные /24) |
| `MIN_PREFIX_LEN` / `MAX_PREFIX_LEN` | фильтр по длине префикса выровненных CIDR |
| `SHARD_SIZE` | размер шарда, по умолчанию **256** |
| `DATABASE_URL` | `postgresql+psycopg://user:pass@localhost:5432/ptr2` |
| `REDIS_URL` | `redis://localhost:6379/0` |
| `USE_STREAMS` | `true` (умолч.) — Redis Streams/consumer groups; `false` — legacy-списки |
| `STREAM_*` / `QUEUE_*` | имена очередей Streams / списков |
| `CONSUMER_GROUP`, `CLAIM_IDLE_MS`, `WORKER_ID` | параметры распределённых воркеров |
| `DNS_NAMESERVERS` | свои resolvers через запятую (пусто = системные) |
| `PTR_CONCURRENCY`, `PTR_RATE_LIMIT` | параллельность и rps PTR-опроса |
| `SPREAD_ENABLED`, `SPREAD_MAX_CONCURRENT`, `FAMILY_BITS` | анти-BAN: рассеивание по /24-семьям |
| `HTTP_CONCURRENCY`, `HTTP_TIMEOUT`, `VERIFY_TLS` | параметры веб-проверок |
| `OWA_PATHS` | пути для OWA-проб |
| `LOG_LEVEL`, `LOG_DIR`, `LOG_TO_FILE` | логирование |

## Схема БД (PostgreSQL)

Инициализация идемпотентна и содержит мягкие миграции: `python -m ptr2.db_schema`
(вызывается автоматически в `setup_env.sh`). Таблицы:

| Таблица | Назначение | Ключ уникальности |
|---|---|---|
| `ripe_ranges` | диапазоны RIPE (inet start/end, cc, size, cidr-выравненность) | `(start_ip, end_ip, type)` |
| `shards` | задачи PTR-обхода по 256 адресов + `family_key` (/24) + статус | `(range_id, shard_index)` |
| `ptr_records` | найденные пары (ip → hostname) | `(ip, hostname)` |
| `a_record_checks` | проверка прямой A-записи (`has_a_record`, `points_back`, `resolved_ips inet[]`) | `(hostname, expected_ip)` |
| `web_checks` | доступность http/https, статус, Server, title, final URL | `(url)` |
| `owa_checks` | детект OWA: `is_owa`, `confidence`, `evidence`, версия Exchange | `(hostname, url, probe_url)` |
| `pipeline_runs` | служебная таблица запусков этапов (stats jsonb) | — |

### Быстрый старт с нуля (БД + Redis без setup_env.sh)

```bash
# PostgreSQL
sudo -u postgres psql -c "CREATE ROLE ptr2 LOGIN PASSWORD 'ptr2';"
sudo -u postgres createdb -O ptr2 ptr2
# Redis
redis-server --daemonize yes
# схема
cp .env.example .env && python -m ptr2.db_schema
```

## Полезные SQL-запросы

```sql
-- где у найденных PTR-хостов есть прямая A-запись:
SELECT hostname, host(expected_ip), array_agg(host(i)) AS a_ips
  FROM a_record_checks, unnest(resolved_ips) i
 WHERE has_a_record GROUP BY 1,2 ORDER BY points_back DESC NULLS LAST;

-- доступные веб-серверы:
SELECT hostname, url, http_status, server_header, title FROM web_checks WHERE reachable;

-- обнаруженный OWA и его версия Exchange:
SELECT hostname, probe_url, confidence, exchange_version, build_number, version_source, evidence
  FROM owa_checks WHERE is_owa ORDER BY confidence;

-- сводка по версиям Exchange:
SELECT COALESCE(exchange_version,'(не определена)') ver, count(DISTINCT hostname) hosts
  FROM owa_checks WHERE is_owa GROUP BY 1 ORDER BY hosts DESC;
```

## Мониторинг очередей Redis

```bash
redis-cli llen ptr2:queue:ptr_shards       # шарды в работе
redis-cli llen ptr2:queue:ptr_results      # PTR-результаты до записи в PG
redis-cli llen ptr2:queue:a_checks
redis-cli llen ptr2:queue:web_checks
redis-cli llen ptr2:queue:owa_checks
```

Если воркер упал mid-flight — задачи остаются в `*:processing`; верните их
запуском с `--reclaim` (или `SimpleQueue.reclaim_stale()`).

## Примечания

- Соблюдайте вежливость к DNS и целевым серверам: крутите `PTR_RATE_LIMIT`, `HTTP_CONCURRENCY`.
- Проверка чужих IP/серверов — только в рамках закона и разрешённого scope.

## FAQ / типовые ошибки

### 1. `env: 'bash\r': No such file or directory`

Причина: shell-скрипты (`run_pipeline.sh`, `setup_env.sh`) были перенесены на
сервер с Windows (через scp/ftp/блокнот или `git config core.autocrlf true`) —
в файле появились Windows-переводы строк `\r\n` (CRLF), и ядро Linux не может
исполнить shebang `#!/usr/bin/env bash\r`.

Исправление (выполнить в каталоге проекта на сервере):

```bash
# все .sh файлы за один проход:
sed -i 's/\r$//' *.sh
# или, если установлен dos2unix:
dos2unix run_pipeline.sh setup_env.sh
chmod +x run_pipeline.sh setup_env.sh
```

Профилактика: в репозитории добавлен `.gitattributes` (`*.sh text eol=lf`);
при копировании архивом с Windows используйте `unzip` (он сохраняет байты
как есть) или сразу выполняйте `sed -i 's/\r$//' *.sh`. Скрипты теперь сами
проверяют свой файл на CRLF и выводят понятную инструкцию вместо непонятной
ошибки `env`.

### 2. `SyntaxError: unmatched ')'` при запуске через Python

Нельзя запускать bash-скрипт интерпретатором Python
(`python run_pipeline.sh`). Правильно:

```bash
./run_pipeline.sh --limit-ranges 5 --ptr-workers 2
# или
bash run_pipeline.sh --limit-ranges 5 --ptr-workers 2
```

Python-скрипты запускаются отдельно: `.venv/bin/python -m ptr2.scripts...`
(см. «Запуск по этапам»).

## Изменения: реконструкция 2026-11-03

- Устранён дубль логирования: `common.setup_logging` (тонкая обёртка) удалён —
  единственный источник: `ptr2/logsetup.py`.
- Очереди Redis сведены в пакет `ptr2/redisq/` (`list_queue.py` — legacy BLMOVE,
  `stream_queue.py` — Streams/consumer groups) с единым реэкспортом имён;
  добавлены хелперы `config.producer()/consumer()` — выбор транспорта (`USE_STREAMS`)
  централизован, обратные имена `stream_producer/stream_consumer` сохранены как алиасы.
- `scripts/_a_payload.py` → `ptr2/apayload.py` (общий формат сообщений).
- `.env.example` создан (раньше отсутствовал, хотя на него ссылался setup_env.sh);
  `.env` добавлен в `.gitignore`; поиск `.env` теперь работает из любого CWD
  (+ переменная `PTR2_ENV_FILE`).
- README: структура проекта, схема БД, полный список переменных, быстрый запуск БД.
- 2026-11-03: проект переименован `iprecon` → `ptr2` (пакет, модули, имя БД/роли
  PostgreSQL, префиксы очередей Redis `ptr2:queue:*` / `ptr2:stream:*`, переменная
  `PTR2_ENV_FILE`). Если в Redis/PG остались данные со старыми именами — при
  перезапуске задайте старые значения через `.env` (`QUEUE_*`, `STREAM_*`,
  `KEY_SHARD_SPREAD`, `DATABASE_URL`) либо начните с чистой очереди.
