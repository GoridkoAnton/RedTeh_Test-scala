from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Параметры, можно переопределить через .env
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[1]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "20g")
SPARK_DRIVER_MEMORY_OVERHEAD = os.environ.get("SPARK_DRIVER_MEMORY_OVERHEAD", "2g")
SPARK_LOCAL_DIRS = os.environ.get("SPARK_LOCAL_DIRS", "/tmp/spark_local")
CONTAINER_MEM_LIMIT = os.environ.get("CONTAINER_MEM_LIMIT", "24g")

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
        app_path = "/app/job.jar"
        app_class = "com.example.SmallFilesAndCompact"
        app_args = f"{subcommand} {' '.join(args)}"

        spark_submit = (
            f"/opt/spark/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            f"--driver-memory {SPARK_DRIVER_MEMORY} "
            f"--conf spark.driver.memoryOverhead={SPARK_DRIVER_MEMORY_OVERHEAD} "
            f"--conf spark.local.dir={SPARK_LOCAL_DIRS} "
            f"--class {app_class} {app_path} "
            f"{app_args}"
        )

        # wrapper: проверка наличия JAR, листинг /app, лог команды и запуск
        wrapper = (
            "set -euo pipefail; "
            f"echo 'Checking for application jar: {app_path}'; "
            f"if [ ! -f '{app_path}' ]; then "
            f"  echo 'ERROR: application jar not found at {app_path}'; "
            f"  echo 'List /app:'; ls -la /app || true; "
            f"  exit 2; "
            f"fi; "
            f"echo 'Contents of /app:'; ls -la /app || true; "
            f"echo 'Running command:'; echo \"{spark_submit}\"; "
            f"{spark_submit}"
        )

        kwargs = {
            "image": "compact-parquet:latest",
            "api_version": "auto",
            "auto_remove": True,
            "entrypoint": ["/bin/sh", "-c"],
            "command": wrapper,
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

        # Монтируем host /data в контейнер, чтобы parquet-файлы попадали на хост
        kwargs["mounts"] = [Mount(source="/data", target="/data", type="bind", read_only=False)]

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