from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Параметры (берутся из окружения или используются значения по умолчанию)
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[1]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "8g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "1g")
SPARK_LOCAL_DIRS = os.environ.get("SPARK_LOCAL_DIRS", "/tmp/spark_local")
CONTAINER_MEM_LIMIT = os.environ.get("CONTAINER_MEM_LIMIT", "11g")
PROJECT_DIR = os.environ.get("PROJECT_DIR")  # необязательно; если задан — будет примонтирован host/data

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

    def make_docker_kwargs(subcommand, args):
        # собираем строку spark-submit — исполняется через shell entrypoint
        # убедитесь, что в образе доступны /opt/spark/bin/spark-submit и /app/job.jar
        spark_submit = (
            f"/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--driver-memory {SPARK_DRIVER_MEMORY} "
            f"--conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} "
            f"--conf spark.local.dir={SPARK_LOCAL_DIRS} "
            f"--class com.example.SmallFilesAndCompact /app/job.jar "
            f"{subcommand} {' '.join(args)}"
        )

        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            # запуск через shell, чтобы в команду вошли все опции
            "entrypoint": ["/bin/sh", "-c"],
            "command": spark_submit,
            "docker_url": "unix://var/run/docker.sock",
            "environment": {
                "SPARK_DRIVER_MEMORY": SPARK_DRIVER_MEMORY,
                "SPARK_DRIVER_MEMORY_OVERHEAD": SPARK_DRIVER_MEMORY_OVERHEAD,
                "SPARK_LOCAL_DIRS": SPARK_LOCAL_DIRS,
                "SPARK_MASTER": SPARK_MASTER,
            },
            "mount_tmp_dir": False,
            "mem_limit": CONTAINER_MEM_LIMIT,
        }

        mounts = []
        # монтируем ./data на /data, если каталог существует относительно рабочей директории compose
        # DockerOperator будет пытаться смонтировать по абсолютному пути, поэтому compose/airflow должно знать PROJECT_DIR
        if PROJECT_DIR:
            host_data = os.path.join(PROJECT_DIR, "data")
            mounts.append(Mount(source=host_data, target="/data", type="bind", read_only=False))
        else:
            # если PROJECT_DIR не задан, пробуем относительный ./data (compose mount настроен на ./data)
            mounts.append(Mount(source=os.path.abspath("./data"), target="/data", type="bind", read_only=False))

        kwargs["mounts"] = mounts
        return kwargs

    gen_kwargs = make_docker_kwargs("generate", ["/data/parquet"])
    gen_kwargs["task_id"] = "generate_parquet"
    generate = DockerOperator(**gen_kwargs)

    comp_kwargs = make_docker_kwargs(
        "compact",
        ["/data/parquet", "50", "jdbc:postgresql://postgres:5432/airflow", "airflow", "airflow"],
    )
    comp_kwargs["task_id"] = "compact_and_register"
    compact = DockerOperator(**comp_kwargs)

    generate >> compact