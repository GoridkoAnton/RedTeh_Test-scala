from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="diagnose_mount_compact_parquet",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    diag_cmd = (
        "set -euo pipefail; "
        "echo '=== DIAGNOSTIC START ==='; "
        "echo 'whoami/uid:'; id || true; "
        "echo 'proc mounts:'; cat /proc/mounts || true; "
        "echo 'df -h /data:'; df -h /data || true; "
        "echo 'ls -la /data before:'; ls -la /data || true; "
        "echo 'ls -la /app:'; ls -la /app || true; "
        "sleep 2; "
        "echo '=== DIAGNOSTIC END ==='; "
        "sleep 300"  # оставляем контейнер живым для ручного inspect
    )

    diagnose = DockerOperator(
        task_id="diagnose_mount",
        image="compact-parquet:latest",
        api_version="auto",
        auto_remove=False,  # важно: контейнер останется после выполнения
        entrypoint=["/bin/sh", "-c"],
        command=diag_cmd,
        docker_url="unix://var/run/docker.sock",
        mounts=[Mount(source="/data", target="/data", type="bind", read_only=False)],
        environment={
            "SPARK_DRIVER_MEMORY": os.environ.get("SPARK_DRIVER_MEMORY", "20g"),
            "SPARK_DRIVER_MEMORY_OVERHEAD": os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "2g"),
            "SPARK_LOCAL_DIRS": os.environ.get("SPARK_LOCAL_DIRS", "/tmp/spark_local"),
        },
        mount_tmp_dir=False,
    )