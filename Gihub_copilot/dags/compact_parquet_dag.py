from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_DIR = os.environ.get("PROJECT_DIR")  # если задан — будет примонтирован host/data и host/data/spark_tmp
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME")

# Рекомендуемые значения для хоста ~12GB
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "6g")              # JVM heap
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "2g")  # вне-heap
CONTAINER_MEM_LIMIT = os.environ.get("CONTAINER_MEM_LIMIT", "11g")             # cgroup limit контейнера
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[1]")                      # уменьшённый параллелизм
SPARK_LOCAL_DIRS = os.environ.get("SPARK_LOCAL_DIRS", "/tmp/spark_local")      # примонтированный tmp на хосте

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
        env = {
            "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
            "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
            "SPARK_MASTER": SPARK_MASTER,
            "SPARK_LOCAL_DIRS": SPARK_LOCAL_DIRS,
        }

        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            "command": command_list,
            "docker_url": "unix://var/run/docker.sock",
            "environment": env,
            "mount_tmp_dir": False,
            "mem_limit": CONTAINER_MEM_LIMIT,
        }

        mounts = []
        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            # mount host data -> /data (your dataset)
            mounts.append(Mount(source=os.path.join(host_data), target="/data", type="bind", read_only=False))
            # mount host tmp for spark local dirs -> /tmp/spark_local
            host_spark_tmp = os.path.join(host_data, "spark_tmp")
            mounts.append(Mount(source=host_spark_tmp, target=SPARK_LOCAL_DIRS, type="bind", read_only=False))

        if mounts:
            kwargs["mounts"] = mounts

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