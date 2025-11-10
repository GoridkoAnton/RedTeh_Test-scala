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
    schedule=None,  # можно задать крон, например "0 3 * * *"
    catchup=False,
    tags=["spark", "gist", "compact"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_spark_job = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="airnet",
        auto_remove=False,        # оставляем контейнер, чтобы можно было смотреть логи
        tty=True,
        mount_tmp_dir=False,

        # монтируем общий docker volume shared_data внутрь контейнера /data
        mounts=[
            Mount(source="shared_data", target="/data", type="volume"),
        ],

        # выполняем spark-submit как в Dockerfile
        command="/opt/spark/bin/spark-submit "
                "--class com.example.SparkCompactJob "
                "--master local[*] "
                "--driver-class-path /opt/postgresql-jdbc.jar "
                "--jars /opt/postgresql-jdbc.jar "
                "/opt/job/target/scala-2.12/spark-compact-job-assembly.jar",

        environment={
            "SPARK_HOME": "/opt/spark",
            "DATA_DIR": "/data/parquet",     # путь будет общий, т.к. это volume
            "TARGET_FILE_MB": "64",
            # ссылка на gist (RAW!)
            "GIST_URL": "https://gist.githubusercontent.com/oerasov/1905065dc6c0133267ec2a8167318399/raw/SmallFiles.scala",
            # параметры генератора
            "SMALLFILES_COLS": "10",
            "SMALLFILES_ROWS": "4000000",
            "SMALLFILES_PARTITIONS": "30",
            # база airflow (если нужно сохранять метаданные)
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "airflow",
            "PG_USER": "airflow",
            "PG_PASSWORD": "airflow",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_spark_job >> end
