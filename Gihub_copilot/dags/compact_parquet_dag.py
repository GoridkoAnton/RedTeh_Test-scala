from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Не задаём жёсткий путь — берём только из окружения, если задано.
# Если PROJECT_DIR не задано, DAG выполнится без bind-mount (контейнер будет писать внутрь образа).
PROJECT_DIR = os.environ.get("PROJECT_DIR")  # <-- убрали жёсткий путь
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

    # Подготовим kwargs динамически — чтобы не передавать None в DockerOperator
    def make_docker_kwargs(command):
        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            "command": command,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {"SPARK_DRIVER_MEMORY": "6g"},
        }

        # Если указан PROJECT_DIR в окружении Airflow, используем bind-mount host:data -> container:/data
        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            # Не создаём каталоги здесь принудительно, но можно раскомментировать, если нужно:
            # os.makedirs(host_data, exist_ok=True)
            data_mount = Mount(source=host_data, target="/data", type="bind", read_only=False)
            kwargs["mounts"] = [data_mount]

        # Если задана сеть — передаём network_mode, иначе не передаём (DockerOperator использует дефолт)
        if NETWORK_NAME:
            kwargs["network_mode"] = NETWORK_NAME

        return kwargs

    gen_kwargs = make_docker_kwargs("generate /data/parquet")
    gen_kwargs["task_id"] = "generate_parquet"
    generate = DockerOperator(**gen_kwargs)

    comp_kwargs = make_docker_kwargs(
        "compact /data/parquet 50 jdbc:postgresql://postgres:5432/airflow airflow airflow"
    )
    comp_kwargs["task_id"] = "compact_and_register"
    compact = DockerOperator(**comp_kwargs)

    generate >> compact