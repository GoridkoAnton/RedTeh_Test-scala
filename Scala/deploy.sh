#!/usr/bin/env bash
set -euo pipefail

# deploy.sh — поднимает postgres + airflow без постоянной сборки Scala-проекта.
# Опции:
#   --build / --rebuild-image  : принудительно (re)build compact-parquet image перед поднятием
#   --help                     : показать помощь

REBUILD_IMAGE=false

for arg in "$@"; do
  case "$arg" in
    --build|--rebuild-image) REBUILD_IMAGE=true ;;
    --help) echo "Usage: $0 [--build|--rebuild-image]"; exit 0 ;;
    *) echo "Unknown arg: $arg"; echo "Usage: $0 [--build|--rebuild-image]"; exit 1 ;;
  esac
done

ENVFILE=".env"

# 1) Генерация Fernet-key и базовых переменных в .env если ещё нет
if [ -f "$ENVFILE" ] && grep -q '^AIRFLOW__CORE__FERNET_KEY=' "$ENVFILE"; then
  echo ".env already contains AIRFLOW__CORE__FERNET_KEY — оставляем как есть."
else
  echo "Генерируем новый Fernet key и записываем в $ENVFILE..."
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
  echo "Записал $ENVFILE"
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

# 3) Проверяем наличие образа compact-parquet:latest
IMAGE_NAME="compact-parquet:latest"
if $REBUILD_IMAGE; then
  echo "--build указан: образ будет пересобран."
  NEED_BUILD=true
else
  if sudo docker image inspect "$IMAGE_NAME" >/dev/null 2>&1; then
    echo "Docker image $IMAGE_NAME found locally — пропускаем сборку."
    NEED_BUILD=false
  else
    echo "Docker image $IMAGE_NAME not found locally."
    echo "Если хотите, выполните './deploy.sh --build' чтобы собрать образ перед запуском."
    NEED_BUILD=false
  fi
fi

# 4) Поднимаем только postgres и airflow (без compact-parquet)
echo "Поднимаем только postgres и airflow (compact-parquet не будет запущен автоматически)..."
sudo docker compose down || true
# Поднимаем нужные сервисы (без сборки compact-parquet)
sudo docker compose up -d --no-deps --build postgres airflow

# 5) Если нужно и запрошено, собираем образ (опционально)
if [ "$NEED_BUILD" = true ]; then
  echo "Building $IMAGE_NAME..."
  # Собираем только образ compact-parquet через compose (использует Dockerfile в контексте)
  sudo docker compose build compact-parquet
  echo "Build finished."
else
  echo "Образ компакт-паркет не был пересобран (NEED_BUILD=false)."
fi

echo "Готово. Airflow и Postgres подняты."
echo
echo "Примечания:"
echo "- compact-parquet сервис в docker-compose.yml оставлен, но не запускается автоматически."
echo "- Если Airflow должен запускать задачу, он ожидает наличия образа '$IMAGE_NAME'."
echo "  * Если образ уже собран — всё готово, DockerOperator внутри Airflow будет запускать контейнер."
echo "  * Если образ не собран и вы хотите собрать сейчас, выполните: ./deploy.sh --build"
echo "- Если хотите, можно добавить задачу в DAG, которая при отсутствии образа вызывает его сборку (требует docker client внутри Airflow)."
echo
echo "Для просмотра логов Airflow:"
echo "  sudo docker compose logs -f airflow --tail 200"