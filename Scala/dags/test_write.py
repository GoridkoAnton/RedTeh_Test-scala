import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime

with DAG(
    dag_id="debug_volume_once",
    start_date=datetime(2025, 11, 12),
    schedule_interval=None,
    catchup=False,
) as dag:
    probe = DockerOperator(
        task_id="probe_write",
        image="alpine:3.20",
        entrypoint=["/bin/sh","-c"],
        command="sh -lc 'id; mkdir -p /data && date > /data/_from_dag.txt && ls -la /data'",
        docker_url="unix://var/run/docker.sock",
        network_mode=os.environ.get("COMPOSE_NETWORK","scala_default"),
        mounts=[Mount(source="parquet_data", target="/data", type="volume")],
        auto_remove=True,
        mount_tmp_dir=False,
        tty=False,
        user="0",   # на время проверки прав
    )
