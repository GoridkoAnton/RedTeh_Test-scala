from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# ---- Configuration (control via env) ----
# IMPORTANT: HOST_DATA_DIR is the path ON THE DOCKER HOST that will be bind-mounted
HOST_DATA_DIR = os.environ.get("HOST_DATA_DIR", "/data")  # <-- host path (you asked to use /data)
DATA_DIR = os.environ.get("DATA_DIR", "/data/parquet")    # path inside container
COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "scala_default")
JOB_IMAGE = os.environ.get("JOB_IMAGE", "compact-parquet:latest")
JOB_JAR_PATH = os.environ.get("JOB_JAR_PATH", "/app/job.jar")
POSTGRES_JDBC = os.environ.get("POSTGRES_JDBC", "jdbc:postgresql://postgres:5432/airflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")

DEBUG = os.environ.get("DEBUG", "") == "1"
KEEP_CONTAINERS = DEBUG or (os.environ.get("KEEP_CONTAINERS", "") == "1")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 11, 12),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

def make_command(mode: str) -> str:
    if mode == "generate":
        job_args = f"generate {DATA_DIR}"
    else:
        job_args = f"compact {DATA_DIR} 50 {POSTGRES_JDBC} {POSTGRES_USER} {POSTGRES_PASSWORD}"

    spark_submit = (
        f"/opt/spark/bin/spark-submit "
        f"--master local[1] "
        f"--driver-memory 4g "
        f"--conf spark.local.dir=/tmp/spark_local "
        f"--class com.example.SmallFilesAndCompact {JOB_JAR_PATH} {job_args}"
    )

    if DEBUG:
        return (
            "set -eux; "
            "echo '=== DIAGNOSTIC START ==='; "
            "echo 'UID/GID:'; id || true; "
            "echo 'PWD:'; pwd || true; "
            "echo 'proc mounts for /data:'; cat /proc/mounts | grep ' /data ' || true; "
            "echo 'df -h /data:'; df -h /data || true; "
            "echo 'ls -la /data BEFORE:'; ls -la /data || true; "
            "echo 'touch marker BEFORE:'; date +%s > /data/_probe_before_$(date +%s) || true; ls -la /data | tail -n 20 || true; "
            f"echo 'RUNNING SPARK: {mode}'; {spark_submit} 2>&1 | tee /data/spark_{mode}_$(date +%s).log || true; "
            "echo 'ls -la /data AFTER:'; ls -la /data || true; "
            "echo 'touch marker AFTER:'; date +%s > /data/_probe_after_$(date +%s) || true; "
            "echo '=== DIAGNOSTIC END ==='; "
        )
    else:
        return f"set -eu; {spark_submit}"

# volumes= list (host bind by default)
host_path = os.path.abspath(HOST_DATA_DIR)
volumes = [f"{host_path}:/data:rw"]

with DAG(
    dag_id="compact_parquet_docker_use_volumes_hostdata",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["docker", "spark"],
) as dag:

    common_kwargs = dict(
        image=JOB_IMAGE,
        api_version="auto",
        entrypoint=["/bin/sh", "-c"],
        docker_url="unix://var/run/docker.sock",
        network_mode=COMPOSE_NETWORK,
        volumes=volumes,    # <-- use volumes= (bind or named)
        mount_tmp_dir=False,
        tty=False,
    )

    gen_kwargs = dict(common_kwargs)
    gen_kwargs["command"] = make_command("generate")
    if DEBUG:
        gen_kwargs.update({"user": "0", "auto_remove": False})
    else:
        gen_kwargs.update({"auto_remove": True})

    generate = DockerOperator(
        task_id="generate_parquet",
        **gen_kwargs,
    )

    cmp_kwargs = dict(common_kwargs)
    cmp_kwargs["command"] = make_command("compact")
    if DEBUG:
        cmp_kwargs.update({"user": "0", "auto_remove": False})
    else:
        cmp_kwargs.update({"auto_remove": True})

    compact = DockerOperator(
        task_id="compact_and_register",
        **cmp_kwargs,
    )

    generate >> compact