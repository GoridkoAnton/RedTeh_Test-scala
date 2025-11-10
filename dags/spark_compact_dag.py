from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="spark_compact_daily",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,                # при необходимости: "0 14 * * *"
    catchup=False,
    tags=["spark", "gist", "compact"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_spark_job = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="airnet",             # <-- СЕТЬ ВОЗВРАЩЕНА
        auto_remove=False,                 # оставляем контейнер для просмотра логов
        tty=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(source="shared_data", target="/data", type="volume"),
        ],
        command=(
            "/opt/spark/bin/spark-submit "
            "--class com.example.SparkCompactJob "
            "--master local[*] "
            "--driver-class-path /opt/postgresql-jdbc.jar "
            "--jars /opt/postgresql-jdbc.jar "
            "/opt/job/target/scala-2.12/spark-compact-job-assembly.jar"
        ),
        environment={
            "SPARK_HOME": "/opt/spark",
            "DATA_DIR": "/data/parquet",
            "TARGET_FILE_MB": "64",
            # RAW-ссылка на твой Gist
            "GIST_URL": "https://gist.githubusercontent.com/oerasov/1905065dc6c0133267ec2a8167318399/raw/SmallFiles.scala",
            "SMALLFILES_COLS": "10",
            "SMALLFILES_ROWS": "4000000",
            "SMALLFILES_PARTITIONS": "30",
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "airflow",
            "PG_USER": "airflow",
            "PG_PASSWORD": "airflow",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_spark_job >> end
