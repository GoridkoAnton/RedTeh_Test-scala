from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

# Параметры — можно переопределять через environment / .env
COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "scala_default")  # замените при необходимости
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[1]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "20g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "2g")
JOB_IMAGE = os.environ.get("JOB_IMAGE", "compact-parquet:latest")
JOB_JAR_PATH = os.environ.get("JOB_JAR_PATH", "/app/job.jar")
DATA_DIR = os.environ.get("DATA_DIR", "/data/parquet")
POSTGRES_JDBC = os.environ.get("POSTGRES_JDBC", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 12),   # дата старта (поставьте в прошлое, чтобы scheduler запланировал первый запуск)
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="compact_parquet_bash_autoremove",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # каждый день в 02:00 UTC; замените на cron по ТЗ если нужно
    catchup=False,                  # не выполнять пропущенные запуски в прошлом
    max_active_runs=1,
    tags=["docker", "bash"],
) as dag:

    generate_cmd = f"""
set -eu
echo "=== GENERATE START ==="
echo "Image: {JOB_IMAGE}"
echo "Writing to host path: {DATA_DIR}"
docker run --rm --name compact_generate_{{{{ ts_nodash }}}} \
  --network {COMPOSE_NETWORK} \
  -v /data:/data \
  --entrypoint /bin/sh {JOB_IMAGE} -c '\
    set -eu; \
    /opt/spark/bin/spark-submit --master {SPARK_MASTER} \
      --driver-memory {SPARK_DRIVER_MEMORY} \
      --conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} \
      --class com.example.SmallFilesAndCompact {JOB_JAR_PATH} generate {DATA_DIR} \
'
echo "=== GENERATE DONE ==="
"""

    generate = BashOperator(
        task_id="generate_parquet_via_docker_cli",
        bash_command=generate_cmd,
        env={"COMPOSE_NETWORK": COMPOSE_NETWORK},
    )

    compact_cmd = f"""
set -eu
echo "=== COMPACT START ==="
echo "Image: {JOB_IMAGE}"
docker run --rm --name compact_compact_{{{{ ts_nodash }}}} \
  --network {COMPOSE_NETWORK} \
  -v /data:/data \
  --entrypoint /bin/sh {JOB_IMAGE} -c '\
    set -eu; \
    /opt/spark/bin/spark-submit --master {SPARK_MASTER} \
      --driver-memory {SPARK_DRIVER_MEMORY} \
      --conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} \
      --class com.example.SmallFilesAndCompact {JOB_JAR_PATH} compact {DATA_DIR} 50 {POSTGRES_JDBC} {POSTGRES_USER} {POSTGRES_PASSWORD} \
'
echo "=== COMPACT DONE ==="
"""

    compact = BashOperator(
        task_id="compact_and_register_via_docker_cli",
        bash_command=compact_cmd,
        env={"COMPOSE_NETWORK": COMPOSE_NETWORK},
    )

    generate >> compact