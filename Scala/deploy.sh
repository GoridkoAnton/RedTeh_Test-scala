#!/usr/bin/env bash
set -euo pipefail

# Скрипт автоматизирует генерацию Fernet ключа, создание .env, /data и поднятие compose.
# Запускается в корне репозитория.

# 1) Генерация Fernet ключа (если уже есть, не перезаписываем)
ENVFILE=".env"
if [ -f "$ENVFILE" ] && grep -q '^AIRFLOW__CORE__FERNET_KEY=' "$ENVFILE"; then
  echo ".env already contains AIRFLOW__CORE__FERNET_KEY, skipping generation."
else
  echo "Generating new Fernet key..."
  NEW_KEY=$(python3 - <<'PY'
import base64,os
print(base64.urlsafe_b64encode(os.urandom(32)).decode())
PY
)
  # Записываем базовые переменные в .env (перезапись .env)
  cat > "$ENVFILE" <<EOF
AIRFLOW__CORE__FERNET_KEY=$NEW_KEY
SPARK_DRIVER_MEMORY=20g
SPARK_DRIVER_MEMORY_OVERHEAD=2g
CONTAINER_MEM_LIMIT=24g
SPARK_MASTER=local[4]
EOF
  echo "Wrote new Fernet key and defaults to $ENVFILE"
fi

# 2) Создать /data на хосте (для parquet) — использовать root права
if [ ! -d /data ]; then
  echo "Creating host directory /data ..."
  sudo mkdir -p /data
  sudo chmod 0775 /data
  echo "/data created and permissions set"
else
  echo "/data already exists, leaving as-is"
fi

# 3) Поднять docker-compose
echo "Bringing up docker compose (build where needed)..."
sudo docker compose down || true
sudo docker compose up -d --build

echo "Waiting a bit for services to stabilize..."
sleep 5

echo "Tailing airflow logs (press Ctrl+C to stop)..."
sudo docker compose logs -f airflow --tail 200