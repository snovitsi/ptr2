#!/usr/bin/env bash
# =============================================================================
# setup_env.sh — подготовка окружения ptr2 БЕЗ Docker.
#
#   1) Проверяет/устанавливает системные пакеты (PostgreSQL, Redis) — по желанию;
#   2) Создаёт Python venv и ставит зависимости из requirements.txt;
#   3) Создаёт в PostgreSQL роль/БД ptr2 (если их нет);
#   4) Запускает локальные сервисы redis-server и postgresql;
#   5) Инициализирует схему БД (ptr2.db_schema).
#
# Использование:
#   ./setup_env.sh              # полная подготовка
#   ./setup_env.sh --no-apt     # не трогать apt (пакеты уже установлены)
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


cd "$(dirname "$0")"

VENV=${VENV:-.venv}
PG_DB=${PG_DB:-ptr2}
PG_USER=${PG_USER:-ptr2}
PG_PASS=${PG_PASS:-ptr2}
NO_APT=0
[[ "${1:-}" == "--no-apt" ]] && NO_APT=1

info() { echo -e "\033[1;36m[ptr2]\033[0m $*"; }
warn() { echo -e "\033[1;33m[ptr2][WARN]\033[0m $*"; }
err()  { echo -e "\033[1;31m[ptr2][ERROR]\033[0m $*" >&2; exit 1; }

# --- 1. системные пакеты -----------------------------------------------------
if [[ $NO_APT -eq 0 ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    need_pkgs=()
    command -v redis-server >/dev/null 2>&1 || need_pkgs+=(redis-server)
    command -v pg_isready   >/dev/null 2>&1 || need_pkgs+=(postgresql-client)
    ls /usr/lib/postgresql/*/bin/postgres >/dev/null 2>&1 || need_pkgs+=(postgresql)
    python3 -m pip --version >/dev/null 2>&1 || need_pkgs+=(python3-venv python3-pip)
    if [[ ${#need_pkgs[@]} -gt 0 ]]; then
      info "Устанавливаю пакеты: ${need_pkgs[*]}"
      sudo apt-get update -y && sudo apt-get install -y "${need_pkgs[@]}"
    else
      info "Системные пакеты уже на месте."
    fi
  else
    warn "apt-get не найден — пропуск установки пакетов. Убедитесь, что postgres и redis установлены вручную."
  fi
fi

# --- 2. python venv + зависимости --------------------------------------------
# ВАЖНО: python3-venv/debinstall может отсутствовать (без apt или в docker-образах
# без него) — проверяем до создания venv и ставим доступными способами.
if ! python3 -m venv --help >/dev/null 2>&1; then
  info "Модуль venv недоступен — пробую установить python3-venv"
  if command -v apt-get >/dev/null 2>&1; then
    (sudo apt-get update -y && sudo apt-get install -y python3-venv python3-pip) || \
      warn "apt-установка python3-venv не удалась"
  fi
fi
if [[ ! -d "$VENV" ]]; then
  info "Создаю виртуальное окружение $VENV"
  if ! python3 -m venv "$VENV"; then
    # запасной вариант: virtualenv через pipx/pip (работает без python3-venv)
    if python3 -m pip install -q --user virtualenv 2>/dev/null || \
       python3 -m pip install -q virtualenv 2>/dev/null; then
      info "Использую virtualenv вместо venv"
      python3 -m virtualenv "$VENV"
    else
      err "Не удалось создать виртуальное окружение: установите python3-venv "\
          "(apt/dnf/apk) или virtualenv (pip) вручную."
    fi
  fi
fi
info "Устанавливаю python-зависимости"
"$VENV/bin/pip" install --upgrade pip -q
"$VENV/bin/pip" install -q -r requirements.txt

# --- 3. .env ------------------------------------------------------------------
if [[ ! -f .env && -f .env.example ]]; then
  info "Копирую .env.example -> .env (localhost без Docker)"
  cp .env.example .env
fi
# читаем DATABASE_URL / REDIS_URL из .env (если есть) — проверки ниже должны
# смотреть туда же, куда смотрит приложение, а не жёстко на 127.0.0.1
[[ -f .env ]] && set +u && source .env 2>/dev/null || true; set -u 2>/dev/null || true
DATABASE_URL=${DATABASE_URL:-postgresql+psycopg://ptr2:ptr2@localhost:5432/ptr2}
REDIS_URL=${REDIS_URL:-redis://localhost:6379/0}

# Парсим host/port из URL вида scheme://[user:pass@]host:port/db
parse_hostport() { # $1=url $2=порт_по_умолчанию -> глобалы PH_HOST PH_PORT
  local rest=${1#*://}
  rest=${rest#*@}                 # отбрасываем user:pass@
  PH_HOST=${rest%%/*}             # host:port (до первого /)
  PH_HOST=${PH_HOST%%\?*}         # на случай query-параметров
  if [[ "$PH_HOST" == *:* ]]; then
    PH_PORT=${PH_HOST##*:}; PH_HOST=${PH_HOST%:*}
  else
    PH_PORT=$2
  fi
  [[ "$PH_HOST" == "" ]] && PH_HOST=localhost
}

# --- 4. запуск сервисов --------------------------------------------------------
# Локальный сервис поднимаем через service/systemctl только если хост БД — локальный.
FORCE_LOCAL_SERVICES=${FORCE_LOCAL_SERVICES:-false}
PG_IS_LOCAL=false; REDIS_IS_LOCAL=false

start_services() {
  # PostgreSQL
  parse_hostport "$DATABASE_URL" 5432
  PG_HOST=$PH_HOST; PG_PORT=$PH_PORT
  case "$PG_HOST" in localhost|127.0.0.1|::1|localhost.localdomain) PG_IS_LOCAL=true ;; esac
  $FORCE_LOCAL_SERVICES && PG_IS_LOCAL=true
  if ! pg_isready -h "$PG_HOST" -p "$PG_PORT" -q 2>/dev/null; then
    if $PG_IS_LOCAL; then
      info "Запускаю PostgreSQL ($PG_HOST:$PG_PORT)"
      if command -v service >/dev/null 2>&1; then
        sudo service postgresql start || true
      elif command -v systemctl >/dev/null 2>&1; then
        sudo systemctl start postgresql || true
      fi
      for _ in $(seq 1 30); do pg_isready -h "$PG_HOST" -p "$PG_PORT" -q && break; sleep 1; done
    else
      warn "PostgreSQL на '$PG_HOST' недоступен с этого хоста — проверьте сеть/брандмауэр/pg_hba.conf."
    fi
  fi
  pg_isready -h "$PG_HOST" -p "$PG_PORT" -q || { warn "PostgreSQL недоступен ($PG_HOST:$PG_PORT) — поднимите его вручную."; exit 1; }

  # Redis
  parse_hostport "$REDIS_URL" 6379
  RD_HOST=$PH_HOST; RD_PORT=$PH_PORT
  case "$RD_HOST" in localhost|127.0.0.1|::1|localhost.localdomain) REDIS_IS_LOCAL=true ;; esac
  if ! timeout 5 redis-cli -h "$RD_HOST" -p "$RD_PORT" ping 2>/dev/null | grep -q PONG; then
    if $REDIS_IS_LOCAL; then
      info "Запускаю Redis ($RD_HOST:$RD_PORT)"
      if command -v service >/dev/null 2>&1; then
        sudo service redis-server start || true
      elif command -v systemctl >/dev/null 2>&1; then
        sudo systemctl start redis-server || true
      fi
      sleep 1
    else
      warn "Redis на '$RD_HOST' недоступен с этого хоста — проверьте сеть/брандмауэр/bind."
    fi
  fi
  timeout 5 redis-cli -h "$RD_HOST" -p "$RD_PORT" ping 2>/dev/null | grep -q PONG \
    || { warn "Redis недоступен ($RD_HOST:$RD_PORT) — поднимите его вручную."; exit 1; }
  info "PostgreSQL ($PG_HOST:$PG_PORT) и Redis ($RD_HOST:$RD_PORT) работают."
}
start_services

# --- 5. роль и база ------------------------------------------------------------
# Роль/БД создаём локально (sudo -u postgres) только если PostgreSQL реально локальный
# (FORCE_LOCAL_SERVICES может принудительно поднять сервисы, но не даёт прав на чужой хост).
PG_TRULY_LOCAL=false
case "$PG_HOST" in localhost|127.0.0.1|::1|localhost.localdomain) PG_TRULY_LOCAL=true ;; esac
if $PG_TRULY_LOCAL; then
  # Если в .env указан внешний DATABASE_URL, но на этом хосте тоже стоит PostgreSQL,
  # не даём случайному sudo-созданию роли «затенить» удалённую базу: убеждаемся, что
  # локальный кластер слушает порт из URL. Если нет — пропускаем (роль на сервере).
  if ! pg_isready -h 127.0.0.1 -p "$PG_PORT" -q 2>/dev/null; then
    warn "На localhost:$PG_PORT нет PostgreSQL — роль/БД на нём не создаём (внешний сервер или другой порт)."
  else
    info "Проверяю роль/БД '$PG_USER' в локальном PostgreSQL"
    sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$PG_USER'" | grep -q 1 || \
      sudo -u postgres psql -c "CREATE ROLE $PG_USER LOGIN PASSWORD '$PG_PASS';"
    sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='$PG_DB'" | grep -q 1 || \
      sudo -u postgres createdb -O "$PG_USER" "$PG_DB"
    # если роль уже есть, но пароля нет — задаём
    sudo -u postgres psql -c "ALTER ROLE $PG_USER LOGIN PASSWORD '$PG_PASS';" >/dev/null
  fi
else
  info "PostgreSQL удалённый ($PG_HOST) — роль/БД создаются администратором сервера, пропускаю."
fi

# --- 6. схема -------------------------------------------------------------------
info "Инициализация схемы БД"
"$VENV/bin/python" -m ptr2.db_schema

info "Готово. Дальше:"
echo "  source $VENV/bin/activate"
echo "  ./run_pipeline.sh          # весь пайплайн"
echo "  или по этапам: python -m ptr2.scripts.00_fetch_ripe ..."
