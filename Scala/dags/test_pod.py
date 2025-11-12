from datetime import datetime
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="debug_keep",
    start_date=datetime(2025, 11, 12),
    schedule_interval=None,
    catchup=False,
) as dag:
    keep = DockerOperator(
        task_id="keep_running",
        image="alpine:3.20",
        # ВАЖНО: не задаём entrypoint, команду даём списком
        command=["sh","-lc","set -eux; id; echo HELLO; mkdir -p /data; date > /data/_from_keep.txt; ls -la /data; sleep 600"],
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("COMPOSE_NETWORK","scala_default"),
        mounts=[Mount(source="parquet_data", target="/data", type="volume")],
        auto_remove=False,       # чтобы контейнер остался висеть
        mount_tmp_dir=False,
        tty=False,
        user="0",               # чтобы исключить проблемы прав
    )