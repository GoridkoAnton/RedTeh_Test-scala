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
    schedule="0 14 * * *",  # 14:00 UTC
    catchup=False,
    tags=["spark","compact"],
) as dag:

    begin = EmptyOperator(task_id="begin")

    run_compact = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            # директория с данными будет общей для Airflow и job-контейнера
            # смонтирована в docker-compose как ./data -> /data
        ],
        environment={
            # куда генерировать/читать файлы
            "DATA_DIR": "/data/parquet",
            # желаемый размер выходных файлов (МБ)
            "TARGET_FILE_MB": "64",
            # sum около 300 МБ маленьких файлов генерируем в контейнере
            "GENERATE_TOTAL_MB": "300",
            # JDBC к тому же Postgres
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "airflow",
            "PG_USER": "airflow",
            "PG_PASSWORD": "airflow",
            # опционально: если дадишь ссылку на gist с generateSmallFiles
            "GIST_URL": "https://gist.github.com/oerasov/1905065dc6c0133267ec2a8167318399",  # например: "https://gist.githubusercontent.com/xxx/raw/generateSmallFiles.scala"
        },
        tty=True,
        command=["/opt/job/run.sh"],
        mount_tmp_dir=False,
        # пробрасываем общий том /data в контейнер задачи
        volumes=["${PWD}/data:/data"]
    )

    end = EmptyOperator(task_id="end")

    begin >> run_compact >> end
