🧩 RdTeh Scala / Spark + Airflow
Проект демонстрирует пайплайн на Apache Spark (Scala), запускаемый из Apache Airflow, с сохранением результатов в именованный Docker volume и регистрацией в PostgreSQL.

⚙️ Основные компоненты
Spark-задание (SmallFilesAndCompact) внутри образа compact-parquet:latest:
generate <dir> — создаёт parquet-файлы в /data/parquet;
compact <dir> <maxFiles> <jdbcUrl> <user> <pass> — компактизирует файлы и при необходимости пишет метаданные в БД.
Airflow DAG (compact_parquet_docker), использующий DockerOperator для прямого вызова spark-submit.
PostgreSQL — подключение для регистрации результатов (опционально).
Именованный volume parquet_data, используемый всеми контейнерами.

🧱 Архитектура
docker-compose
├─ airflow
│   ├─ выполняет DAGи
│   ├─ монтирует том parquet_data → /data
│   └─ подключён к сети scala_default
├─ postgres
│   └─ доступен по jdbc:postgresql://postgres:5432/airflow
└─ compact-parquet
    └─ Spark runtime (Spark 3.4.2 + Scala)
Общие данные хранятся в именованном томе parquet_data, видимом как /data в контейнерах.
Airflow запускает spark-submit через DockerOperator, сбрасывая ENTRYPOINT внутри job-контейнера.

🧰 Требования
Docker + Docker Compose v2
Минимум 12 ГБ RAM
Свободное место под Docker volumes
Порты
Сервис	Порт
Airflow UI	8080
PostgreSQL	5432
Spark UI	4040 (только во время выполнения job)

🚀 Быстрый старт (Автоматически)
1️⃣ Выдать права на сокет Docker
Чтобы Airflow мог запускать контейнеры:
sudo chmod 666 /var/run/docker.sock
2️⃣ Клонировать проект
git clone https://github.com/GoridkoAnton/RedTeh_Test-scala.git
cd RedTeh_Test-scala/Scala
3️⃣ Выполнить sudo chmod +x deploy.sh
️4️⃣ Запустить скрипт деплоя sudo ./deploy.sh

🚀 Быстрый старт (Ручной)
1️⃣ Выдать права на сокет Docker
Чтобы Airflow мог запускать контейнеры:
sudo chmod 666 /var/run/docker.sock
2️⃣ Клонировать проект
git clone https://github.com/GoridkoAnton/RedTeh_Test-scala.git
cd RedTeh_Test-scala/Scala
3️⃣ Создать именованный том
docker volume create parquet_data
4️⃣ Поднять окружение
docker compose build compact-parquet docker
docker compose up -d airflow postgresql
5️⃣ Проверить запуск сервисов
docker compose ps
6️⃣ Открыть Airflow UI
http://SERVERIP:8080
7️⃣ Активировать DAG compact_parquet_docker и выполнить задачи

⚙️ Конфигурация
Параметры передаются в Airflow и контейнер job-образа через переменные окружения:
Переменная	Значение по умолчанию
COMPOSE_NETWORK	scala_default
SPARK_MASTER	local[1]
SPARK_DRIVER_MEMORY	20g
SPARK_DRIVER_MEMORY_OVERHEAD	2g
JOB_IMAGE	compact-parquet:latest
JOB_JAR_PATH	/app/job.jar
DATA_DIR	/data/parquet
POSTGRES_JDBC	jdbc:postgresql://postgres:5432/airflow
POSTGRES_USER	airflow
POSTGRES_PASSWORD	airflow
KEEP_CONTAINERS	(пусто — удалять контейнеры после завершения; если 1 — сохранять)

🧩 Запуск вручную (CLI)
# Подготовка тома и прав
docker compose exec airflow bash -lc \
  "airflow tasks run compact_parquet_docker_test prepare_volume \$(date -u +'%Y-%m-%dT%H:%M:%S+00:00') -i --raw"

# Генерация parquet
docker compose exec airflow bash -lc \
  "airflow tasks run compact_parquet_docker_test generate_parquet \$(date -u +'%Y-%m-%dT%H:%M:%S+00:00') -i --raw"

# Компактизация и регистрация
docker compose exec airflow bash -lc \
  "airflow tasks run compact_parquet_docker_test compact_and_register \$(date -u +'%Y-%m-%dT%H:%M:%S+00:00') -i --raw"

📂 Проверка результатов
Том parquet_data виден как /data в контейнерах.
Проверить содержимое:
docker run --rm -v parquet_data:/data alpine:3.20 sh -lc 'ls -la /data && ls -la /data/parquet || true'
или внутри Airflow-контейнера:
docker compose exec airflow bash -lc 'ls -la /data/parquet'
После успешного generate_parquet ожидаются файлы:
part-*.parquet
_SUCCESS
_probe.txt

🔍 Отладка и диагностика
Проверить контейнеры задач:
docker ps -a --filter 'name=compact_parquet' \
  --format 'table {{.ID}}\t{{.Status}}\t{{.Image}}\t{{.Names}}'
Проверить подключение PostgreSQL:
docker run --rm --network scala_default alpine:3.20 \
  sh -lc 'apk add -q busybox-extras >/dev/null; ping -c1 postgres'
Проверить монтирование тома:
CID=$(docker ps -a --filter 'ancestor=compact-parquet:latest' \
  --format '{{.ID}}' | head -n1)
docker inspect "$CID" --format '{{json .Mounts}}' | jq .
Посмотреть логи задач:
docker compose exec airflow bash -lc \
  "airflow tasks logs compact_parquet_docker_test generate_parquet \
  \$(date -u +'%Y-%m-%dT%H:%M:%S+00:00') | tail -100"

🧹 Очистка
# Остановить окружение
docker compose down

# Удалить данные
docker volume rm parquet_data

# Очистить неиспользуемые ресурсы (осторожно!)
docker system prune -f

📘 Примечания
Используется именованный том parquet_data, создаваемый автоматически Docker-ом.
Он хранит все parquet-файлы между запусками.
Чтобы «обнулить» данные — достаточно удалить том:
docker volume rm parquet_data
Airflow DAG запускает контейнеры через Docker API, а не через docker CLI.
Благодаря entrypoint="", spark-submit получает аргументы напрямую.
prepare_volume гарантирует корректные права (50000:0, g+rwX), чтобы Spark имел доступ к данным в томе.
Для локальной отладки можно использовать Bash-вариант DAG (compact_parquet_bash_autoremove.py),
где команды выполняются через docker run.