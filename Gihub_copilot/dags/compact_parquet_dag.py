from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# Настройте эту константу в имя вашей docker-сети созданной docker-compose (обычно <project>_default)
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME", "gihub_copilot_default")

# Абсолютный путь к каталогу проекта на хосте, чтобы DockerOperator мог смонтировать ./data
# Задайте PROJECT_DIR как переменную окружения в Airflow, иначе укажите полный путь здесь.
PROJECT_DIR = os.environ.get("PROJECT_DIR", "/home/youruser/path/to/project")

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

    generate = DockerOperator(
        task_id="generate_parquet",
        image="compact-parquet:latest",
        api_version="auto",
        auto_remove=True,
        command="generate /data/parquet",
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK_NAME,
        environment={"SPARK_DRIVER_MEMORY": "6g"},
        # Bind mount host data directory into the container so files persist on host
        volumes=[f"{PROJECT_DIR}/data:/data"],
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
        volumes=[f"{PROJECT_DIR}/data:/data"],
    )

    generate >> compact