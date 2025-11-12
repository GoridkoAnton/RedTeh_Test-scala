from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ===== Параметры окружения =====
COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "scala_default")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[1]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "20g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "2g")
JOB_IMAGE = os.environ.get("JOB_IMAGE", "compact-parquet:latest")
JOB_JAR_PATH = os.environ.get("JOB_JAR_PATH", "/app/job.jar")
DATA_DIR = os.environ.get("DATA_DIR", "/data/parquet")
POSTGRES_JDBC = os.environ.get("POSTGRES_JDBC", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
KEEP_CONTAINERS = os.environ.get("KEEP_CONTAINERS", "") != ""

# ===== Параметры DAG =====
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="compact_parquet_docker_test",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # ежедневный запуск в 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["docker", "spark"],
) as dag:

    # Используем именованный volume, общий между задачами
    mounts = [Mount(source="parquet_data", target="/data", type="volume")]

    # === Подготовка прав ===
    prep = DockerOperator(
        task_id="prepare_volume",
        image="alpine:3.20",
        command=[
            "/bin/sh",
            "-c",
            "mkdir -p /data/parquet && "
            "chown -R 50000:0 /data && "
            "chmod -R g+rwX /data && "
            "ls -ld /data/parquet",
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        auto_remove=True,
        mount_tmp_dir=False,
        user="0",
    )

    # === Генерация Parquet ===
    generate = DockerOperator(
        task_id="generate_parquet",
        image=JOB_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove=(not KEEP_CONTAINERS),
        user="0",
        entrypoint="",  # сбрасываем ENTRYPOINT контейнера
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", SPARK_MASTER,
            "--driver-memory", SPARK_DRIVER_MEMORY,
            "--conf", f"spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD}",
            "--conf", "spark.local.dir=/tmp/spark_local",
            "--class", "com.example.SmallFilesAndCompact",
            JOB_JAR_PATH,
            "generate", DATA_DIR,
        ],
        environment={
            "SPARK_MASTER": SPARK_MASTER,
            "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
            "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
            "SPARK_LOCAL_DIRS": "/tmp/spark_local",
        },
    )

    # === Компактизация и регистрация ===
    compact = DockerOperator(
        task_id="compact_and_register",
        image=JOB_IMAGE,
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove=(not KEEP_CONTAINERS),
        user="0",
        entrypoint="",  # также обнуляем ENTRYPOINT
        command=[
            "/opt/spark/bin/spark-submit",
            "--master", SPARK_MASTER,
            "--driver-memory", SPARK_DRIVER_MEMORY,
            "--conf", f"spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD}",
            "--conf", "spark.local.dir=/tmp/spark_local",
            "--class", "com.example.SmallFilesAndCompact",
            JOB_JAR_PATH,
            "compact", DATA_DIR, "50", POSTGRES_JDBC, POSTGRES_USER, POSTGRES_PASSWORD,
        ],
        environment={
            "SPARK_MASTER": SPARK_MASTER,
            "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
            "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
            "SPARK_LOCAL_DIRS": "/tmp/spark_local",
        },
    )

    # === Зависимости ===
    prep >> generate >> compact
