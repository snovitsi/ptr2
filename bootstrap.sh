#!/bin/bash
# Быстрая подготовка окружения ptr2 на чистом хосте (Ubuntu/Debian).
# Ставит PostgreSQL + Redis, python venv, зависимости и роль/БД ptr2.
set -euo pipefail
echo "[ptr2] установка системных пакетов..."
sudo apt-get update -y
sudo apt-get install -y postgresql redis-server python3-venv python3-pip curl
./setup_env.sh
