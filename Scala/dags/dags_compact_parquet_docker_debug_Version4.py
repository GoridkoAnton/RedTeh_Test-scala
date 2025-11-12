from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="compact_parquet_docker_debug",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["docker", "debug"],
) as dag:

    def make_command(mode: str) -> str:
        if mode == "generate":
            job_args = f"generate {DATA_DIR}"
        else:
            job_args = f"compact {DATA_DIR} 50 {POSTGRES_JDBC} {POSTGRES_USER} {POSTGRES_PASSWORD}"
        spark_submit = (
            f"/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--driver-memory {SPARK_DRIVER_MEMORY} "
            f"--conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} "
            f"--conf spark.local.dir=/tmp/spark_local "
            f"--class com.example.SmallFilesAndCompact {JOB_JAR_PATH} {job_args}"
        )
        # без pipefail (в образе /bin/sh)
        return (
            "set -eu; "
            "echo '=== INSIDE CONTAINER: BEFORE ==='; ls -la /data || true; "
            f"echo 'Running: {spark_submit}'; {spark_submit}; "
            "echo '=== INSIDE CONTAINER: AFTER ==='; ls -la /data || true;"
        )

    mounts = [Mount(source="/data", target="/data", type="bind", read_only=False)]

    generate = DockerOperator(
        task_id="generate_parquet",
        image=JOB_IMAGE,
        api_version="auto",
        command=make_command("generate"),
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove=False,  # DEBUG: не удалять контейнер, чтобы можно было inspect+logs
        tty=False,
    )

    compact = DockerOperator(
        task_id="compact_and_register",
        image=JOB_IMAGE,
        api_version="auto",
        command=make_command("compact"),
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove=False,  # DEBUG
        tty=False,
    )

    generate >> compact