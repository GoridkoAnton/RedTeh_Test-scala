from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Опциональные переменные окружения:
PROJECT_DIR = os.environ.get("PROJECT_DIR")  # если задан, будет использоваться для монтирования host/data
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME")  # опционально

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

    def make_docker_kwargs(command_list):
        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            # Передаём команду как список, чтобы аргументы точно дошли в контейнер
            "command": command_list,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {"SPARK_DRIVER_MEMORY": "6g"},
            # Отключаем автоматическое монтирование tmp dir (оно вызывало ошибку bind source does not exist)
            "mount_tmp_dir": False,
        }

        # Если задан PROJECT_DIR — примонтируем host:data -> container:/data
        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            data_mount = Mount(source=host_data, target="/data", type="bind", read_only=False)
            kwargs["mounts"] = [data_mount]

        # Если указан NETWORK_NAME — используем его, иначе DockerOperator создаст контейнер
        if NETWORK_NAME:
            kwargs["network_mode"] = NETWORK_NAME

        return kwargs

    gen_kwargs = make_docker_kwargs(["generate", "/data/parquet"])
    gen_kwargs["task_id"] = "generate_parquet"
    generate = DockerOperator(**gen_kwargs)

    comp_kwargs = make_docker_kwargs(
        ["compact", "/data/parquet", "50", "jdbc:postgresql://postgres:5432/airflow", "airflow", "airflow"]
    )
    comp_kwargs["task_id"] = "compact_and_register"
    compact = DockerOperator(**comp_kwargs)

    generate >> compact