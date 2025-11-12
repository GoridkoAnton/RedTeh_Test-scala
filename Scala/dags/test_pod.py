from datetime import datetime
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="debug_mount",
    start_date=datetime(2025, 11, 12),
    schedule_interval=None,
    catchup=False,
) as dag:
    probe = DockerOperator(
        task_id="show_mounts_and_sleep",
        image="alpine:3.20",
        entrypoint=["/bin/sh","-c"],
        command=(
            "set -eux; "
            "id; "
            "echo '= /proc/mounts for /data ='; grep ' /data ' /proc/mounts || true; "
            "echo '= ls -la /data before ='; ls -la /data || true; "
            "mkdir -p /data; date > /data/_from_dag_alive.txt; "
            "echo '= ls -la /data after ='; ls -la /data || true; "
            "sleep 600"
        ),
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("COMPOSE_NETWORK","scala_default"),
        mounts=[Mount(source="parquet_data", target="/data", type="volume")],
        auto_remove=False,
        mount_tmp_dir=False,
        tty=False,
        user="0",
    )
