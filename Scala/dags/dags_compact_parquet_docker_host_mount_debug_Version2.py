from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Host path (важно: это ПУТЬ НА ХОСТЕ)
HOST_DATA_DIR = os.environ.get("HOST_DATA_DIR", "/srv/data")
COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "scala_default")
JOB_IMAGE = os.environ.get("JOB_IMAGE", "compact-parquet:latest")
JOB_JAR_PATH = os.environ.get("JOB_JAR_PATH", "/app/job.jar")
DATA_DIR = os.environ.get("DATA_DIR", "/data/parquet")  # путь внутри контейнера
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

def make_wrapper(mode: str) -> str:
    if mode == "generate":
        job_args = f"generate {DATA_DIR}"
    else:
        job_args = f"compact {DATA_DIR} 50 {POSTGRES_JDBC} {POSTGRES_USER} {POSTGRES_PASSWORD}"
    spark_submit = (
        f"/opt/spark/bin/spark-submit "
        f"--master local[1] "
        f"--driver-memory 2g "
        f"--conf spark.local.dir=/tmp/spark_local "
        f"--class com.example.SmallFilesAndCompact {JOB_JAR_PATH} {job_args}"
    )
    # Diagnostic wrapper: prints mounts, touches markers and tees spark output into /data
    return (
        "set -eux; "
        "echo '=== DIAGNOSTIC START ==='; "
        "echo 'UID/GID:'; id || true; "
        "echo 'PWD:'; pwd || true; "
        "echo 'proc mounts lines for /data:'; cat /proc/mounts | grep ' /data ' || true; "
        "echo 'df -h /data:'; df -h /data || true; "
        "echo 'ls -la /data BEFORE:'; ls -la /data || true; "
        "echo 'touch marker BEFORE:'; date +%s > /data/_probe_before_$(date +%s) || true; ls -la /data | tail -n 20 || true; "
        f"echo 'RUNNING SPARK: {mode}'; {spark_submit} 2>&1 | tee /data/spark_{mode}_$(date +%s).log; "
        "echo 'ls -la /data AFTER:'; ls -la /data || true; "
        "echo 'touch marker AFTER:'; date +%s > /data/_probe_after_$(date +%s) || true; "
        "echo '=== DIAGNOSTIC END ==='; "
    )

with DAG(
    dag_id="compact_parquet_docker_host_mount_debug",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["docker", "debug"],
) as dag:

    mounts = [Mount(source=os.path.join(HOST_DATA_DIR), target="/data", type="bind", read_only=False)]

    generate = DockerOperator(
        task_id="generate_parquet",
        image=JOB_IMAGE,
        api_version="auto",
        command=make_wrapper("generate"),
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        user="0",                       # временно: запуск от root для исключения проблем с правами
        auto_remove=False if KEEP_CONTAINERS else False,
        tty=False,
    )

    compact = DockerOperator(
        task_id="compact_and_register",
        image=JOB_IMAGE,
        api_version="auto",
        command=make_wrapper("compact"),
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        mounts=mounts,
        mount_tmp_dir=False,
        user="0",
        auto_remove=False if KEEP_CONTAINERS else False,
        tty=False,
    )

    generate >> compact