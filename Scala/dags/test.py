from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ===== Параметры =====
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

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

# ===== Команда с диагностикой =====
def make_wrapper(mode: str, extra_args: str = "") -> str:
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
        f"--class com.example.SmallFilesAndCompact {JOB_JAR_PATH} {job_args} {extra_args}"
    )

    return (
        "set -eu; "
        "echo '=== DIAGNOSTIC START ==='; "
        "id; grep \" /data \" /proc/mounts || true; "
        "echo 'ls -la /data (root):'; ls -la /data || true; "
        f"echo 'mkdir -p {DATA_DIR} and probe write'; "
        f"mkdir -p {DATA_DIR} && date +%s > {DATA_DIR}/_probe.txt && ls -la {DATA_DIR} || true; "
        f"echo 'Running: {spark_submit}'; {spark_submit}; "
        f"echo 'ls -la {DATA_DIR} (after):'; ls -la {DATA_DIR} || true; "
        "echo '=== DIAGNOSTIC END ===';"
    )

# ===== DAG =====
with DAG(
    dag_id="compact_parquet_docker",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # ежедневно в 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["docker", "spark"],
) as dag:

    mounts = [Mount(source="parquet_data", target="/data", type="volume")]

    # Подготовка прав (однократно перед Spark)
    prep = DockerOperator(
        task_id="prepare_volume",
        image="alpine:3.20",
        command=["sh", "-lc", "mkdir -p /data/parquet && chown -R 50000:0 /data && chmod -R g+rwX /data && ls -ld /data/parquet"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        auto_remove=True,
        mount_tmp_dir=False,
        tty=False,
        user="0",
    )

    generate = DockerOperator(
        task_id="generate_parquet",
        image=JOB_IMAGE,
        api_version="auto",
        command=["sh", "-lc", make_wrapper("generate")],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove=(not KEEP_CONTAINERS),
        tty=False,
        user="0",
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
        command=["sh", "-lc", make_wrapper("compact")],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        auto_remove=(not KEEP_CONTAINERS),
        tty=False,
        user="0",
        environment={
            "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
            "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
            "SPARK_MASTER": SPARK_MASTER,
        },
    )

    prep >> generate >> compact
