from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="spark_compact_daily",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,                 # включай вручную; хочешь по крону — поменяй, например "0 14 * * *"
    catchup=False,
    tags=["spark", "compact"],
) as dag:

    begin = EmptyOperator(task_id="begin")

    run_compact = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="airnet",      # та же сеть, что в compose
        auto_remove=False,          # оставляем контейнер после завершения для просмотра логов
        mount_tmp_dir=False,        # никаких временных томов Airflow
        tty=True,

        # Команда запуска — без .sh, чтобы Airflow не пытался открыть как шаблон
        command="/opt/job/run",

        # Переменные окружения для Spark job
        environment={
            "DATA_DIR": "/data/parquet",      # внутри контейнера job; без внешнего тома данные будут эфемерные
            "TARGET_FILE_MB": "64",
            "GENERATE_TOTAL_MB": "300",
            "GIST_URL": "",
            "PG_HOST": "postgres",            # сервис Postgres из docker-compose
            "PG_PORT": "5432",
            "PG_DB": "airflow",
            "PG_USER": "airflow",
            "PG_PASSWORD": "airflow",
        },
    )

    end = EmptyOperator(task_id="end")

    begin >> run_compact >> end
