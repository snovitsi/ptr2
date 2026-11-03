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
if [[ ! -d "$VENV" ]]; then
  info "Создаю виртуальное окружение $VENV"
  python3 -m venv "$VENV"
fi
info "Устанавливаю python-зависимости"
"$VENV/bin/pip" install --upgrade pip -q
"$VENV/bin/pip" install -q -r requirements.txt

# --- 3. .env ------------------------------------------------------------------
if [[ ! -f .env && -f .env.example ]]; then
  info "Копирую .env.example -> .env (localhost без Docker)"
  cp .env.example .env
fi

# --- 4. запуск сервисов --------------------------------------------------------
start_services() {
  # PostgreSQL
  if ! pg_isready -h 127.0.0.1 -q 2>/dev/null; then
    info "Запускаю PostgreSQL"
    if command -v service >/dev/null 2>&1; then
      sudo service postgresql start || true
    elif command -v systemctl >/dev/null 2>&1; then
      sudo systemctl start postgresql || true
    fi
    for _ in $(seq 1 30); do pg_isready -h 127.0.0.1 -q && break; sleep 1; done
  fi
  pg_isready -h 127.0.0.1 -q || { warn "PostgreSQL недоступен — поднимите его вручную."; exit 1; }

  # Redis
  if ! redis-cli -h 127.0.0.1 ping >/dev/null 2>&1; then
    info "Запускаю Redis"
    if command -v service >/dev/null 2>&1; then
      sudo service redis-server start || true
    elif command -v systemctl >/dev/null 2>&1; then
      sudo systemctl start redis-server || true
    fi
    sleep 1
  fi
  redis-cli -h 127.0.0.1 ping >/dev/null 2>&1 || { warn "Redis недоступен — поднимите его вручную."; exit 1; }
  info "PostgreSQL и Redis работают."
}
start_services

# --- 5. роль и база ------------------------------------------------------------
info "Проверяю роль/БД '$PG_USER' в PostgreSQL"
sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$PG_USER'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE ROLE $PG_USER LOGIN PASSWORD '$PG_PASS';"
sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='$PG_DB'" | grep -q 1 || \
  sudo -u postgres createdb -O "$PG_USER" "$PG_DB"
# если роль уже есть, но пароля нет — задаём
sudo -u postgres psql -c "ALTER ROLE $PG_USER LOGIN PASSWORD '$PG_PASS';" >/dev/null

# --- 6. схема -------------------------------------------------------------------
info "Инициализация схемы БД"
"$VENV/bin/python" -m ptr2.db_schema

info "Готово. Дальше:"
echo "  source $VENV/bin/activate"
echo "  ./run_pipeline.sh          # весь пайплайн"
echo "  или по этапам: python -m ptr2.scripts.00_fetch_ripe ..."
