from datetime import datetime
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Опционально: задавайте PROJECT_DIR и (при необходимости) COMPOSE_NETWORK_NAME в окружении Airflow.
PROJECT_DIR = os.environ.get("PROJECT_DIR")  # если не задан — контейнер выполнится без bind-mount к хосту
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME")  # опционально

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "6g")
SPARK_SUBMIT_BASE = (
    "/opt/spark/bin/spark-submit --master local[4] "
    f"--driver-memory {SPARK_DRIVER_MEMORY} "
    "--class com.example.SmallFilesAndCompact /app/job.jar"
)

with DAG(
    dag_id="compact_parquet_and_register",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    def make_docker_kwargs(command_str):
        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            # Используем shell entrypoint, чтобы корректно выполнить сложную команду
            "entrypoint": ["/bin/sh", "-c"],
            # command передаём строкой (shell выполнит spark-submit ...)
            "command": command_str,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {"SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY},
            # Отключаем tmp-mount (исправляет bind source path does not exist)
            "mount_tmp_dir": False,
        }

        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            data_mount = Mount(source=host_data, target="/data", type="bind", read_only=False)
            kwargs["mounts"] = [data_mount]

        if NETWORK_NAME:
            kwargs["network_mode"] = NETWORK_NAME

        return kwargs

    gen_cmd = f"{SPARK_SUBMIT_BASE} generate /data/parquet"
    gen_kwargs = make_docker_kwargs(gen_cmd)
    gen_kwargs["task_id"] = "generate_parquet"
    generate = DockerOperator(**gen_kwargs)

    comp_cmd = (
        f"{SPARK_SUBMIT_BASE} compact /data/parquet 50 jdbc:postgresql://postgres:5432/airflow airflow airflow"
    )
    comp_kwargs = make_docker_kwargs(comp_cmd)
    comp_kwargs["task_id"] = "compact_and_register"
    compact = DockerOperator(**comp_kwargs)

    generate >> compact