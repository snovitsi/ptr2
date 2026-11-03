#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh — запуск всего пайплайна ptr2 БЕЗ Docker, локально.
#
#   00 fetch      : скачивание delegated-ripencc-latest + загрузка в PostgreSQL
#   01 shard      : отбор диапазонов (страна/размер) -> шарды по 256 IP -> Redis
#   02 ptr        : PTR-обход всех адресов шардов (несколько воркеров параллельно)
#   03 load_ptr   : вычитка результатов PTR из Redis -> таблица ptr_records
#   04 check_a    : проверка прямой A-записи у найденных хостнеймов
#   05 check_web  : доступность веб-серверов (http/https) по хостнеймам с A-записью
#   06 check_owa  : детекция Outlook Web Access за доступными серверами
#   07 report     : сводный отчёт
#
# Логирование: каждый этап пишет logs/<этап>.log (статусы INFO, предупреждения
# WARNING, ошибки ERROR с traceback), а краткие статусы/ошибки этапов — одной
# строкой — дублируются в logs/pipeline.log. Каталог настраивается LOG_DIR.
#
# Использование:
#   ./run_pipeline.sh                 # полный прогон
#   ./run_pipeline.sh --limit-ranges 5 --ptr-workers 2   # быстрый тест
#   SKIP=fetch,shard ./run_pipeline.sh                   # пропустить этапы
#   ONLY=report ./run_pipeline.sh                        # только отчёт
#   LOG_DIR=/var/log/ptr2 ./run_pipeline.sh           # свой каталог логов
# =============================================================================
set -euo pipefail

# Защита от CRLF (Windows-переводы строк при переносе файлов scp/ftp/Notepad++
# или git core.autocrlf). Если скрипт всё же дошёл до исполнения (например,
# `bash run_pipeline.sh`), выдаём понятную инструкцию вместо непонятных ошибок.
if LC_ALL=C grep -q "$(printf '\r')" "$0" 2>/dev/null; then
  echo "ОШИБКА: в '$0' Windows-переводы строк (CRLF). Исправьте:" >&2
  echo "    sed -i 's/\r$//' *.sh        # или: dos2unix *.sh" >&2
  echo "Подробности — README.md, раздел FAQ." >&2
  exit 1
fi


# Защита от типичной ошибки: этот файл — Bash-скрипт, его нельзя запускать
# интерпретатором Python (python run_pipeline.sh -> SyntaxError на case/;;).
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  echo "ОШИБКА: '$0' запускается НЕ через bash." >&2
  echo "run_pipeline.sh — это Bash-скрипт. Правильно:" >&2
  echo "    ./run_pipeline.sh [флаги]     # или:  bash run_pipeline.sh [флаги]" >&2
  echo "Неправильно (так делать нельзя):" >&2
  echo "    python run_pipeline.sh        # <- вызывает SyntaxError в bash-секциях" >&2
  exit 1
fi

cd "$(dirname "$0")"

ORIG_ARGS=("$@")   # сохраняем исходную строку аргументов до разбора

# --help / -h: краткая справка пайплайна (без реального прогона этапов)
for a in "${ORIG_ARGS[@]}"; do
  if [[ "$a" == "--help" || "$a" == "-h" ]]; then
    sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
    echo
    echo "Флаги пайплайна:"
    echo "  --limit-ranges N    ограничить число диапазонов (быстрый тест)"
    echo "  --ptr-workers K     параллельных PTR-воркеров (по умолчанию PTR_WORKERS или 4)"
    echo "Переменные окружения:"
    echo "  SKIP=a,b            пропустить этапы (fetch,shard,ptr,load_ptr,check_a,check_web,owa,report)"
    echo "  ONLY=a,b            выполнить только перечисленные этапы"
    echo "  LOG_DIR=./logs      каталог логов"
    echo "Пример: ./run_pipeline.sh --limit-ranges 5 --ptr-workers 2"
    exit 0
  fi
done

VENV=${VENV:-.venv}
PY="$VENV/bin/python"
[[ -x "$PY" ]] || PY=$(command -v python3)

PTR_WORKERS=${PTR_WORKERS:-4}
LIMIT_RANGES=${LIMIT_RANGES:-}          # напр. 5 — ограничить число диапазонов (тест)
SKIP=${SKIP:-}                          # список пропускаемых этапов через запятую
ONLY=${ONLY:-}                          # если задан — выполнить только эти этапы

# общий лог пайплайна (каталог должен совпадать с LOG_DIR, который читает python)
export LOG_DIR="${LOG_DIR:-./logs}"
mkdir -p "$LOG_DIR"
PIPELINE_LOG="$LOG_DIR/pipeline.log"
RUN_T0=$(date +%s)

stage_event() { # stage_event <stage> <STATUS> [k=v ...]
  local ts host line
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  host=$(hostname -s 2>/dev/null || echo unknown)
  line="$ts host=$host stage=$1 status=$2"
  shift 2
  for kv in "$@"; do line="$line $kv"; done
  echo "$line" >> "$PIPELINE_LOG"
}

parse_opt() { # достаёт значение флага из исходной командной строки
  local prev="" a
  for a in "${ORIG_ARGS[@]}"; do
    if [[ "$prev" == "$1" ]]; then echo "$a"; return; fi
    prev="$a"
  done
}
v=$(parse_opt "--ptr-workers"); [[ -n "$v" ]] && PTR_WORKERS=$v
v=$(parse_opt "--limit-ranges"); [[ -n "$v" ]] && LIMIT_RANGES=$v

# остальные аргументы прозрачно передаются этапам (кроме флагов пайплайна и их значений)
EXTRA_ARGS=()
skip_next=0
for a in "${ORIG_ARGS[@]}"; do
  if [[ $skip_next -eq 1 ]]; then skip_next=0; continue; fi
  case "$a" in
    --ptr-workers|--limit-ranges) skip_next=1 ;;
    *) EXTRA_ARGS+=("$a") ;;
  esac
done

should_run() {
  local stage=$1
  if [[ -n "$ONLY" ]]; then [[ ",$ONLY," == *",$stage,"* ]]; return; fi
  [[ ",$SKIP," == *",$stage,"* ]] && return 1
  return 0
}

banner() { echo -e "\n\033[1;35m==== $1 ====\033[0m"; }

# run_stage <stage> <команда...>: логирует START/DONE/FAIL в pipeline.log
run_stage() {
  local stage=$1; shift
  local t0 rc
  t0=$(date +%s)
  stage_event "$stage" START
  if "$@"; then rc=0; else rc=$?; fi
  local dur=$(( $(date +%s) - t0 ))
  if [[ $rc -eq 0 ]]; then
    stage_event "$stage" DONE seconds=$dur
  else
    stage_event "$stage" FAIL seconds=$dur exit=$rc
    echo "[ERROR] этап $stage завершился с кодом $rc (см. $LOG_DIR/$stage*.log, $PIPELINE_LOG)" >&2
  fi
  return $rc
}

if should_run fetch; then
  banner "00. Скачивание RIPE и загрузка диапазонов в PostgreSQL"
  run_stage fetch "$PY" -m ptr2.scripts.00_fetch_ripe "${EXTRA_ARGS[@]}"
fi

if should_run shard; then
  banner "01. Отбор диапазонов по критериям и разбиение на шарды по 256 адресов"
  SHARD_ARGS=()
  [[ -n "$LIMIT_RANGES" ]] && SHARD_ARGS+=(--max-ranges "$LIMIT_RANGES")
  run_stage shard "$PY" -m ptr2.scripts.01_select_and_shard "${SHARD_ARGS[@]}" "${EXTRA_ARGS[@]}"
fi

if should_run ptr; then
  banner "02. PTR-обход адресов ($PTR_WORKERS воркеров параллельно)"
  stage_event ptr_worker START workers=$PTR_WORKERS
  PIDS=()
  for i in $(seq 1 "$PTR_WORKERS"); do
    WORKER_TAG="-$(printf '%02d' "$i")" \
      "$PY" -m ptr2.scripts.02_ptr_worker --once --reclaim &
    PIDS+=($!)
  done
  FAIL=0
  for pid in "${PIDS[@]}"; do wait "$pid" || FAIL=1; done
  if [[ $FAIL -eq 0 ]]; then
    stage_event ptr_worker DONE workers=$PTR_WORKERS
  else
    stage_event ptr_worker FAIL workers=$PTR_WORKERS note="часть воркеров завершилась с ошибкой"
    echo "[WARN] часть PTR-воркеров завершилась с ошибкой (см. $LOG_DIR/ptr_worker-*.log)" >&2
  fi
fi

if should_run load_ptr; then
  banner "03. Запись найденных PTR-записей в PostgreSQL"
  run_stage load_ptr "$PY" -m ptr2.scripts.03_load_ptr --once
fi

if should_run check_a; then
  banner "04. Проверка прямых A-записей у обнаруженных хостнеймов"
  run_stage check_a "$PY" -m ptr2.scripts.04_check_a --from-queue --once
fi

if should_run check_web; then
  banner "05. Проверка доступности веб-серверов"
  run_stage check_web "$PY" -m ptr2.scripts.05_check_web --from-queue --once
fi

if should_run check_owa; then
  banner "06. Поиск Outlook Web Access (OWA)"
  if [[ "${USE_STREAMS:-true}" == "false" ]]; then
    # legacy-списки: один воркер (--from-queue)
    run_stage owa "$PY" -m ptr2.scripts.06_check_owa --from-queue
  else
    # streams: несколько потребителей одной consumer group (параллельно)
    OWA_WORKERS=${OWA_WORKERS:-2}
    stage_event owa_worker START workers=$OWA_WORKERS
    PIDS=()
    for i in $(seq 1 "$OWA_WORKERS"); do
      WORKER_TAG="-$(printf '%02d' "$i")" \
        "$PY" -m ptr2.scripts.06_check_owa --from-queue --once &
      PIDS+=($!)
    done
    FAIL=0
    for pid in "${PIDS[@]}"; do wait "$pid" || FAIL=1; done
    if [[ $FAIL -eq 0 ]]; then
      stage_event owa_worker DONE workers=$OWA_WORKERS
    else
      stage_event owa_worker FAIL workers=$OWA_WORKERS note="часть OWA-воркеров завершилась с ошибкой"
      echo "[WARN] часть OWA-воркеров завершилась с ошибкой (см. $LOG_DIR/owa*.log)" >&2
    fi
  fi
fi

if should_run report; then
  banner "07. Сводный отчёт"
  run_stage report "$PY" -m ptr2.scripts.07_report
fi

RUN_DUR=$(( $(date +%s) - RUN_T0 ))
banner "Пайплайн завершён за ${RUN_DUR}с"
echo -e "\033[1;36mЛоги: $LOG_DIR/ (pipeline.log — статусы и ошибки этапов)\033[0m"
grep -c "status=FAIL" "$PIPELINE_LOG" >/dev/null 2>&1 && \
  grep "status=FAIL" "$PIPELINE_LOG" | tail -5 | sed "s/^/[FAIL] /" || true
