from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Укажите имя docker-сети вашего docker-compose (обычно <project>_default)
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME", "gihub_copilot_default")

# Укажите абсолютный путь к корню проекта на хосте или задайте PROJECT_DIR в environment Airflow
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/home/redteh/project/RedTeh_Test-scala/Gihub_copilot")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="compact_parquet_and_register",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Примонтировать host:${PROJECT_DIR}/data -> container:/data через docker.types.Mount
    data_mount = Mount(source=f"{PROJECT_DIR}/data", target="/data", type="bind", read_only=False)

    generate = DockerOperator(
        task_id="generate_parquet",
        image="compact-parquet:latest",
        api_version="auto",
        auto_remove=True,
        command="generate /data/parquet",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_NAME,
        environment={"SPARK_DRIVER_MEMORY": "6g"},
        mounts=[data_mount],
    )

    compact = DockerOperator(
        task_id="compact_and_register",
        image="compact-parquet:latest",
        api_version="auto",
        auto_remove=True,
        command="compact /data/parquet 50 jdbc:postgresql://postgres:5432/airflow airflow airflow",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_NAME,
        environment={"SPARK_DRIVER_MEMORY": "6g"},
        mounts=[data_mount],
    )

    generate >> compact