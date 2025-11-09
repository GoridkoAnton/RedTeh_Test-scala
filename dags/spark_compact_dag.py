from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="spark_compact_daily",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 14 * * *",  # запуск каждый день в 14:00 UTC
    catchup=False,
    tags=["spark", "compact"],
) as dag:

    begin = EmptyOperator(task_id="begin")

    run_compact = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",

        # Используем mounts (а не volumes)
        mounts=[
            Mount(source="shared_data", target="/data", type="volume"),
        ],

        # Переменные окружения передаются в контейнер Spark job
        environment={
            "DATA_DIR": "/data/parquet",       # каталог с входными/выходными parquet
            "TARGET_FILE_MB": "64",            # размер выходных файлов
            "GENERATE_TOTAL_MB": "300",        # общий объём генерируемых данных
            "GIST_URL": "",                    # URL на gist, если нужно загрузить генератор
            "PG_HOST": "postgres",             # параметры подключения к Postgres
            "PG_PORT": "5432",
            "PG_DB": "airflow",
            "PG_USER": "airflow",
            "PG_PASSWORD": "airflow",
        },

        tty=True,
        command="/opt/job/run.sh",             # ← теперь строка, не список
        mount_tmp_dir=False,
        template_fields=[],                    # ← отключаем шаблонизацию Jinja
    )

    end = EmptyOperator(task_id="end")

    # Порядок выполнения DAG
    begin >> run_compact >> end
