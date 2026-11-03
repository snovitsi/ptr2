#!/usr/bin/env bash
# bootstrap.sh — полная подготовка хоста под ptr2 (Debian/Ubuntu):
# ставит PostgreSQL + Redis + python-venv/pip, затем вызывает ./setup_env.sh
# (создание venv, установка зависимостей, роль/БД ptr2, схема).
set -euo pipefail
echo "[ptr2] Устанавливаю системные пакеты (postgresql, redis-server, python3-venv, python3-pip)"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y postgresql redis-server python3-venv python3-pip curl ca-certificates
echo "[ptr2] Запускаю setup_env.sh"
exec ./setup_env.sh "$@"
