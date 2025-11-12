from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="compact_parquet_bash_run",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["debug"],
) as dag:

    # Запускает контейнер для генерации parquet и оставляет его живым (sleep) для отладки.
    generate = BashOperator(
        task_id="generate_parquet_via_docker_cli",
        bash_command="""
docker run --name compact_generate_debug -v /data:/data \
  --entrypoint /bin/sh compact-parquet:latest -c \
  '/opt/spark/bin/spark-submit --master local[1] \
   --driver-memory 20g --conf spark.driver.memoryOverhead=2g \
   --class com.example.SmallFilesAndCompact /app/job.jar generate /data/parquet && \
   echo GENERATE_DONE; sleep 600' \
|| (echo "Container run failed; dumping logs:"; docker logs compact_generate_debug || true; exit 1)
""",
    )

    # Запускает контейнер для compact + регистрация и оставляет его живым (sleep) для отладки.
    compact = BashOperator(
        task_id="compact_and_register_via_docker_cli",
        bash_command="""
docker run --name compact_compact_debug -v /data:/data \
  --entrypoint /bin/sh compact-parquet:latest -c \
  '/opt/spark/bin/spark-submit --master local[1] \
   --driver-memory 20g --conf spark.driver.memoryOverhead=2g \
   --class com.example.SmallFilesAndCompact /app/job.jar compact /data/parquet 50 jdbc:postgresql://postgres:5432/airflow airflow airflow && \
   echo COMPACT_DONE; sleep 600' \
|| (echo "Container run failed; dumping logs:"; docker logs compact_compact_debug || true; exit 1)
""",
    )

    generate >> compact