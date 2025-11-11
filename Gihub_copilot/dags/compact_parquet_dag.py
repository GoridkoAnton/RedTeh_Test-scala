from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_DIR = os.environ.get("PROJECT_DIR")
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME")

# Настройки для host с ~12GB RAM
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "8g")   # JVM driver
CONTAINER_MEM_LIMIT = os.environ.get("CONTAINER_MEM_LIMIT", "10g")  # cgroup limit для контейнера
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")          # параллелизм

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
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
            "command": command_list,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {"SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY, "SPARK_MASTER": SPARK_MASTER},
            "mount_tmp_dir": False,
            "mem_limit": CONTAINER_MEM_LIMIT,
        }

        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            data_mount = Mount(source=host_data, target="/data", type="bind", read_only=False)
            kwargs["mounts"] = [data_mount]

        if NETWORK_NAME:
            kwargs["network_mode"] = NETWORK_NAME

        return kwargs

    gen_cmd = ["generate", "/data/parquet"]
    gen_kwargs = make_docker_kwargs(gen_cmd)
    gen_kwargs["task_id"] = "generate_parquet"
    generate = DockerOperator(**gen_kwargs)

    comp_cmd = ["compact", "/data/parquet", "50", "jdbc:postgresql://postgres:5432/airflow", "airflow", "airflow"]
    comp_kwargs = make_docker_kwargs(comp_cmd)
    comp_kwargs["task_id"] = "compact_and_register"
    compact = DockerOperator(**comp_kwargs)

    generate >> compact