#!/usr/bin/env bash
set -euo pipefail

# Wait for Postgres to be ready
RETRIES=60
SLEEP=2
i=0
until pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-airflow}" >/dev/null 2>&1; do
  i=$((i+1))
  if [ $i -ge $RETRIES ]; then
    echo "Postgres is not ready after $((RETRIES * SLEEP)) seconds"
    exit 1
  fi
  echo "Waiting for Postgres... ($i/$RETRIES)"
  sleep $SLEEP
done

echo "Postgres is ready, initializing Airflow metadata DB if needed..."

# Initialize DB (safe to call multiple times)
airflow db init

# Create admin user if not exists (idempotent-ish)
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Finally start scheduler and webserver (scheduler in background, webserver in foreground)
airflow scheduler &

# exec so signals are forwarded to webserver
exec airflow webserver