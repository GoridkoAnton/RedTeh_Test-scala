from datetime import datetime
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Optional: set PROJECT_DIR and COMPOSE_NETWORK_NAME via Airflow environment if needed.
# If PROJECT_DIR is set, host/PROJECT_DIR/data will be bind-mounted into the task container at /data.
PROJECT_DIR = os.environ.get("PROJECT_DIR")
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "6g")

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
            # Pass the application arguments as a list; entrypoint in the image will run spark-submit
            "command": command_list,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {"SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY},
            # Do not mount Airflow task tmp dir into the created container
            "mount_tmp_dir": False,
        }

        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            data_mount = Mount(source=host_data, target="/data", type="bind", read_only=False)
            kwargs["mounts"] = [data_mount]

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