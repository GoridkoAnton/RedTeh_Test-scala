from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Параметры (можно переопределять через env)
COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "scala_default")  # имя сети docker-compose
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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="compact_parquet_docker",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # ежедневно в 02:00 (UTC)
    catchup=False,
    max_active_runs=1,
    tags=["docker", "spark"],
) as dag:

    def make_command(mode: str, extra_args: str = "") -> str:
        # mode: "generate" или "compact"
        if mode == "generate":
            job_args = f"generate {DATA_DIR}"
        else:
            # compact expects: <src_dir> <max_files> <jdbc_url> <user> <pass>
            job_args = f"compact {DATA_DIR} 50 {POSTGRES_JDBC} {POSTGRES_USER} {POSTGRES_PASSWORD}"

        spark_submit = (
            f"/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--driver-memory {SPARK_DRIVER_MEMORY} "
            f"--conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} "
            f"--conf spark.local.dir=/tmp/spark_local "
            f"--class com.example.SmallFilesAndCompact {JOB_JAR_PATH} {job_args} {extra_args}"
        )

        # Диагностика + запуск
        wrapper = (
            "set -eu; "
            "echo '=== DIAGNOSTIC START ==='; "
            "echo 'id:'; id; "
            "echo 'mounts:'; cat /proc/mounts | grep ' /data ' || true; "
            "echo 'ls -la /data (before):'; ls -la /data || true; "
            "echo 'probe write:'; date +%s > /data/_probe.txt && ls -la /data/_probe.txt || true; "
            f"echo 'Running: {spark_submit}'; {spark_submit}; "
            "echo 'ls -la /data (after):'; ls -la /data || true; "
            "echo '=== DIAGNOSTIC END ===';"
        )
        return wrapper

    # ВАЖНО: используем volumes, чтобы точно примонтировать /data хоста в /data контейнера
    docker_volumes = ["/data:/data:rw"]

    generate = DockerOperator(
        task_id="generate_parquet",
        image=JOB_IMAGE,
        api_version="auto",
        command=make_command("generate"),
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        volumes=docker_volumes,
        mount_tmp_dir=False,
        auto_remove=(not KEEP_CONTAINERS),
        tty=False,
        get_logs=True,
        user="0",  # можно убрать после проверки прав на /data
        environment={
            "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
            "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
            "SPARK_MASTER": SPARK_MASTER,
        },
    )

    compact = DockerOperator(
        task_id="compact_and_register",
        image=JOB_IMAGE,
        api_version="auto",
        command=make_command("compact"),
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        volumes=docker_volumes,
        mount_tmp_dir=False,
        auto_remove=(not KEEP_CONTAINERS),
        tty=False,
        get_logs=True,
        user="0",  # можно убрать после проверки прав на /data
        environment={
            "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
            "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
            "SPARK_MASTER": SPARK_MASTER,
        },
    )

    generate >> compact
