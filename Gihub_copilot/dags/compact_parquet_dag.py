from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Берём конфиг из окружения (можно положить эти переменные в docker-compose -> service airflow -> environment)
PROJECT_DIR = os.environ.get("PROJECT_DIR")  # например: /abs/path/to/project
NETWORK_NAME = os.environ.get("COMPOSE_NETWORK_NAME")

# Параметры (подстраивай под хост)
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[1]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "7g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "1g")
SPARK_LOCAL_DIRS = os.environ.get("SPARK_LOCAL_DIRS", "/tmp/spark_local")
CONTAINER_MEM_LIMIT = os.environ.get("CONTAINER_MEM_LIMIT", "11g")

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

    def make_docker_kwargs(subcommand, args_list):
        # формируем команду spark-submit как одну shell-строку
        spark_submit = (
            f"/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--driver-memory {SPARK_DRIVER_MEMORY} "
            f"--conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} "
            f"--conf spark.local.dir={SPARK_LOCAL_DIRS} "
            f"--class com.example.SmallFilesAndCompact /app/job.jar "
            f"{subcommand} {' '.join(args_list)}"
        )

        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            # запускаем через shell, чтобы собрать всю строку spark-submit
            "entrypoint": ["/bin/sh", "-c"],
            "command": spark_submit,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {
                "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
                "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
                "SPARK_LOCAL_DIRS": SPARK_LOCAL_DIRS,
            },
            "mount_tmp_dir": False,
            "mem_limit": CONTAINER_MEM_LIMIT,
        }

        mounts = []
        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            # монтируем данные
            mounts.append(Mount(source=host_data, target="/data", type="bind", read_only=False))
            # и отдельную папку для spark.local.dir (для spill'а)
            host_spark_tmp = os.path.join(host_data, "spark_tmp")
            mounts.append(Mount(source=host_spark_tmp, target=SPARK_LOCAL_DIRS, type="bind", read_only=False))

        if mounts:
            kwargs["mounts"] = mounts

        if NETWORK_NAME:
            kwargs["network_mode"] = NETWORK_NAME

        return kwargs

    gen_kwargs = make_docker_kwargs("generate", ["/data/parquet"])
    gen_kwargs["task_id"] = "generate_parquet"
    generate = DockerOperator(**gen_kwargs)

    comp_kwargs = make_docker_kwargs(
        "compact", ["/data/parquet", "50", "jdbc:postgresql://postgres:5432/airflow", "airflow", "airflow"]
    )
    comp_kwargs["task_id"] = "compact_and_register"
    compact = DockerOperator(**comp_kwargs)

    generate >> compact