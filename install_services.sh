#!/usr/bin/env bash
# =============================================================================
# install_services.sh — установка systemd-сервисов и таймеров этапов ptr2.
#
# Делает:
#   1) проверяет окружение (systemd, python>=3.8, .venv, .env);
#   2) создаёт системного пользователя/группу ptr2 (без входа в shell);
#   3) настраивает права на каталоги проекта (data/, logs/ доступны ptr2);
#   4) кладёт юниты из systemd/ в /etc/systemd/system;
#   5) кладёт шаблон конфигурации расписания в /etc/ptr2/ptr2.env;
#   6) systemctl daemon-reload + включает нужные сервисы/таймеры.
#
# Использование (от root):
#   sudo ./install_services.sh                 # установить всё, включить по умолчанию
#   sudo ./install_services.sh --no-enable     # только установить юниты (enable позже)
#   sudo ./install_services.sh --home /srv/ptr2  # проект в другом месте
#   sudo ./install_services.sh --workers 4     # включить 4 PTR-воркера (@1..@4)
#   sudo ./install_services.sh --timers off    # не включать таймерные этапы вовсе
#   sudo ./install_services.sh --remove        # удалить юниты/таймеры (файлы БД не трогает)
#
# Что включается по умолчанию (--enable):
#   ptr2-ptr-worker.service            — долгоживущий PTR-воркер (Restart=always)
#   ptr2-ptr-worker@1..N               — дополнительные воркеры (--workers N+1 всего)
#   ptr2-fetch.timer                   — RIPE ежедневно ~03:15
#   ptr2-load-ptr.timer                — выгрузка PTR каждые 10 мин
#   ptr2-check-a.timer                 — A-записи каждые 15 мин
#   ptr2-check-web.timer               — веб каждые 30 мин
#   ptr2-check-owa.timer               — OWA каждые 45 мин
#   ptr2-report.timer                  — отчёт ежедневно 09:00
#   ptr2-shard.timer НЕ включается (пересев шардов вручную или по флагу --with-shard-timer)
#
# Расписание/параллелизм правится в /etc/ptr2/ptr2.env после установки.
# =============================================================================
set -euo pipefail

if LC_ALL=C grep -q "$(printf '\r')" "$0" 2>/dev/null; then
  echo "ОШИБКА: в '$0' Windows-переводы строк (CRLF). Исправьте: sed -i 's/\r$//' *.sh" >&2
  exit 1
fi

APP_HOME="${IPRECON_HOME:-$(cd "$(dirname "$0")" && pwd)}"
USER_NAME="ptr2"
GROUP_NAME="ptr2"
UNITS_DIR=/etc/systemd/system
CONF_DIR=/etc/ptr2
SRC_UNITS="$(dirname "$0")/systemd"
ENABLE=1
WORKERS=1                       # сколько ptr2-ptr-worker@N включить (основной + доп.)
WITH_SHARD_TIMER=0
MODE=install

info() { echo "[ptr2-services] $*"; }
die()  { echo "[ptr2-services] ОШИБКА: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --home)          APP_HOME="$2"; shift 2 ;;
    --home=*|--app-home=*) APP_HOME="${1#*=}"; shift ;;
    --no-enable)     ENABLE=0; shift ;;
    --timers)        [[ "${2:-}" == "off" ]] && ENABLE=0 || die "--timers принимает off"; shift 2 ;;
    --workers)       WORKERS="$2"; shift 2 ;;
    --workers=*)     WORKERS="${1#*=}"; shift ;;
    --with-shard-timer) WITH_SHARD_TIMER=1; shift ;;
    --remove)        MODE=remove; shift ;;
    -h|--help)       sed -n '2,33p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "неизвестный аргумент: $1 (справка: --help)" ;;
  esac
done

[[ $EUID -eq 0 ]] || die "запускайте от root: sudo $0"
[[ -d /run/systemd/system ]] || die "systemd не обнаружен (/run/systemd/system отсутствует)"
[[ -f "$APP_HOME/run_pipeline.sh" ]] || die "в '$APP_HOME' нет run_pipeline.sh — укажите путь через --home"

# ---------------------------------------------------------------- remove -----
if [[ "$MODE" == "remove" ]]; then
  info "Останавливаю и удаляю юниты ptr2..."
  for u in $(systemctl list-unit-files 'ptr2-*' --no-legends --no-legend 2>/dev/null | awk '{print $1}'); do
    systemctl disable --now "$u" 2>/dev/null || true
    rm -f "$UNITS_DIR/$u"
  done
  systemctl daemon-reload
  info "Юниты удалены. Пользователь $USER_NAME, каталоги и /etc/ptr2 — оставлены."
  info "(удалить пользователя: userdel -r $USER_NAME; конфиг: rm -rf $CONF_DIR)"
  exit 0
fi

# --------------------------------------------------------------- checks ------
[[ -x "$APP_HOME/.venv/bin/python" ]] || die "нет venv: выполните сначала ./setup_env.sh в $APP_HOME"
PYVER="$("$APP_HOME/.venv/bin/python" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
info "python в venv: $PYVER"

# ------------------------------------------- пользователь и права на проект --
if ! getent group "$GROUP_NAME" >/dev/null; then
  info "Создаю группу $GROUP_NAME"
  groupadd --system "$GROUP_NAME"
fi
if ! id "$USER_NAME" >/dev/null 2>&1; then
  info "Создаю системного пользователя $USER_NAME (nologin, home=$APP_HOME)"
  useradd --system --gid "$GROUP_NAME" --home-dir "$APP_HOME" \
          --shell /usr/sbin/nologin --no-create-home "$USER_NAME"
else
  info "Пользователь $USER_NAME уже существует"
fi

# Код и venv читаемы для всех, но принадлежат root:root; писать ptr2 может
# только в data/ и logs/ (согласовано с ProtectSystem=full в юнитах).
chgrp -R "$GROUP_NAME" "$APP_HOME/data" "$APP_HOME/logs" 2>/dev/null || true
chmod 770 "$APP_HOME/data" "$APP_HOME/logs" 2>/dev/null || true
mkdir -p "$APP_HOME/data" "$APP_HOME/logs"
chown "$USER_NAME:$GROUP_NAME" "$APP_HOME/data" "$APP_HOME/logs"
chmod 750 "$APP_HOME/data" "$APP_HOME/logs"
# .env содержит DATABASE_URL/REDIS_URL с паролями — доступ только владельцу и группе
if [[ -f "$APP_HOME/.env" ]]; then
  chgrp "$GROUP_NAME" "$APP_HOME/.env"
  chmod 640 "$APP_HOME/.env"
fi

# ------------------------------------------------------- конфиг расписания ---
mkdir -p "$CONF_DIR"
if [[ ! -f "$CONF_DIR/ptr2.env" ]]; then
  info "Устанавливаю $CONF_DIR/ptr2.env (из systemd/ptr2.env)"
  install -m 640 -o root -g "$GROUP_NAME" "$SRC_UNITS/ptr2.env" "$CONF_DIR/ptr2.env"
else
  info "$CONF_DIR/ptr2.env уже существует — оставляю без изменений"
fi
# Подставляем фактический путь проекта, если он отличается от /opt/ptr2
REAL_HOME=$(cd "$APP_HOME" && pwd)
sed -i "s|^IPRECON_HOME=.*|IPRECON_HOME=$REAL_HOME|" "$CONF_DIR/ptr2.env"
sed -i "s|^PTR_WORKERS=.*|PTR_WORKERS=$WORKERS|" "$CONF_DIR/ptr2.env"

# ------------------------------------------------------------- юниты ---------
[[ -d "$SRC_UNITS" ]] || die "нет каталога юнитов $SRC_UNITS"

# Значения из конфига для подстановки в шаблоны ТАЙМЕРОВ (OnCalendar/
# RandomizedDelaySec не читаются из EnvironmentFile — фиксируем их в файле юнита).
# В .service оставляем $VAR — их подставит systemd из EnvironmentFile при запуске.
declare -A VARS=()
while IFS='=' read -r k v; do
  [[ "$k" =~ ^[A-Z_][A-Z0-9_]*$ ]] || continue
  v="${v%%#*}"                       # убрать хвостовой комментарий
  v="${v%"${v##*[![:space:]]}"}"     # убрать пробелы справа
  VARS["$k"]="$v"
done < <(grep -E '^[A-Z_][A-Z0-9_]*=' "$CONF_DIR/ptr2.env")

render_unit() {  # $1 = исходник, $2 = назначение: заменяет ${VAR} на значения из VARS
  local src="$1" dst="$2" line k out=""
  while IFS= read -r line || [[ -n "$line" ]]; do
    while [[ "$line" =~ \$\{([A-Z_][A-Z0-9_]*)\} ]]; do
      k="${BASH_REMATCH[1]}"
      [[ -n "${VARS[$k]+x}" ]] || die "в $src незаполненный параметр \${$k} (задайте его в $CONF_DIR/ptr2.env)"
      line="${line/\${$k\}/${VARS[$k]}}"   # подставляем ВСЮ конструкцию ${VAR} вместе с $
    done
    out+="$line"$'\n'
  done < "$src"
  printf '%s' "$out" > "$dst"
}

shopt -s nullglob
for f in "$SRC_UNITS"/*.service "$SRC_UNITS"/*.timer; do
  base=$(basename "$f")
  dest="$UNITS_DIR/$base"
  if [[ "$base" == *.timer ]]; then
    # OnCalendar/RandomizedDelaySec systemd НЕ раскрывает из EnvironmentFile —
    # подставляем значения из /etc/ptr2/ptr2.env прямо в файл юнита.
    tmp=$(mktemp)
    render_unit "$f" "$tmp"
    if [[ -f "$dest" ]] && cmp -s "$tmp" "$dest"; then
      info "  $base — без изменений"
    else
      info "  устанавливаю $base (расписание из конфига)"
      install -m 644 "$tmp" "$dest"
    fi
    rm -f "$tmp"
  else
    # service: аргументы остаются как $VAR — их подставит systemd из EnvironmentFile
    if [[ -f "$dest" ]] && cmp -s "$f" "$dest"; then
      info "  $base — без изменений"
    else
      info "  устанавливаю $base"
      install -m 644 "$f" "$dest"
    fi
  fi
done
shopt -u nullglob

systemctl daemon-reload
# Проверка синтаксиса установленных юнитов (не критично, но полезно)
if command -v systemd-analyze >/dev/null; then
  bad=0
  for f in "$UNITS_DIR"/ptr2-*.service "$UNITS_DIR"/ptr2-*.timer; do
    systemd-analyze verify --root=/ "$f" >/dev/null 2>&1 || bad=$((bad+1))
  done
  [[ $bad -gt 0 ]] && info "Внимание: systemd-analyze вернул предупреждения для $bad юнитов (обычно из-за отсутствия /opt/... на этой машине до первого запуска)"
fi

# ------------------------------------------------------------- enable --------
if [[ "$ENABLE" -eq 1 ]]; then
  UNITS=("ptr2-ptr-worker.service")
  for ((i=2; i<=WORKERS; i++)); do UNITS+=("ptr2-ptr-worker@$i.service"); done
  UNITS+=("ptr2-fetch.timer" "ptr2-load-ptr.timer" "ptr2-check-a.timer"
          "ptr2-check-web.timer" "ptr2-check-owa.timer" "ptr2-report.timer")
  [[ "$WITH_SHARD_TIMER" -eq 1 ]] && UNITS+=("ptr2-shard.timer")
  info "Включаю: ${UNITS[*]}"
  systemctl enable --now "${UNITS[@]}"
else
  info "Enable пропущен (--no-apply/--timers off). Включите вручную:"
  echo "    systemctl enable --now ptr2-ptr-worker.service ptr2-fetch.timer \\"
  echo "      ptr2-load-ptr.timer ptr2-check-a.timer ptr2-check-web.timer \\"
  echo "      ptr2-check-owa.timer ptr2-report.timer"
fi

cat <<EOF

Готово. Полезные команды:
  systemctl status ptr2-ptr-worker.service          # состояние воркера
  journalctl -u ptr2-load-ptr.service -n 50         # лог конкретного запуска этапа
  systemctl list-timers 'ptr2-*'                    # расписание и ближайшие запуски
  systemctl start ptr2-check-a.service              # запустить этап вручную сейчас
  $EDITOR /etc/ptr2/ptr2.env                        # расписание/параллелизм; затем:
  systemctl daemon-reload && systemctl restart 'ptr2-*'
EOF
