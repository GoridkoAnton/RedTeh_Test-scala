#!/usr/bin/env bash
set -euo pipefail

# deploy.sh — автоматизация для демо:
# - создаёт .env с Fernet ключом (если ещё нет)
# - создаёт /data на хосте (если нужно) и выставляет базовые права
# - поднимает только postgres + airflow (compact-parquet не запускаем автоматически)
# - опция --build: собрать образ compact-parquet перед использованием

REBUILD_IMAGE=false
for arg in "$@"; do
  case "$arg" in
    --build|--rebuild-image) REBUILD_IMAGE=true ;;
    --help) echo "Usage: $0 [--build|--rebuild-image]"; exit 0 ;;
    *) echo "Unknown arg: $arg"; echo "Usage: $0 [--build|--rebuild-image]"; exit 1 ;;
  esac
done

ENVFILE=".env"

# 1) Генерируем Fernet key в .env если ещё нет
if [ -f "$ENVFILE" ] && grep -q '^AIRFLOW__CORE__FERNET_KEY=' "$ENVFILE"; then
  echo ".env already contains AIRFLOW__CORE__FERNET_KEY — оставляем."
else
  echo "Generating new Fernet key and writing to $ENVFILE..."
  NEW_KEY=$(python3 - <<'PY'
import base64,os
print(base64.urlsafe_b64encode(os.urandom(32)).decode())
PY
)
  cat > "$ENVFILE" <<EOF
AIRFLOW__CORE__FERNET_KEY=$NEW_KEY
SPARK_DRIVER_MEMORY=20g
SPARK_DRIVER_MEMORY_OVERHEAD=2g
CONTAINER_MEM_LIMIT=24g
SPARK_MASTER=local[4]
EOF
  echo "Wrote $ENVFILE"
fi

# 2) Ensure /data exists on host (parquet location)
if [ ! -d /data ]; then
  echo "Creating host directory /data ..."
  sudo mkdir -p /data
  sudo chmod 0775 /data
  echo "/data created and permissions set"
else
  echo "/data already exists — leaving as-is"
fi

# 3) Check local compact-parquet image presence (do not build by default)
IMAGE_NAME="compact-parquet:latest"
if $REBUILD_IMAGE; then
  echo "--build specified: will (re)build $IMAGE_NAME later."
  NEED_BUILD=true
else
  if sudo docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "Docker image $IMAGE_NAME found locally — will not rebuild."
    NEED_BUILD=false
  else
    echo "Docker image $IMAGE_NAME not found locally. You can build it with './deploy.sh --build' if needed."
    NEED_BUILD=false
  fi
fi

# 4) Bring up postgres and airflow only
echo "Bringing up postgres + airflow (compact-parquet service is present in compose but not started explicitly here)..."
sudo docker compose down || true
sudo docker compose up -d --no-deps --build postgres airflow

# 5) Optionally build compact-parquet (only if requested)
if [ "$NEED_BUILD" = true ]; then
  echo "Building $IMAGE_NAME..."
  sudo docker compose build compact-parquet
  echo "Build complete."
fi

echo "Deployment finished. Tail Airflow logs with:"
echo "  sudo docker compose logs -f airflow --tail 200"