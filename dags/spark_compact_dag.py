from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="spark_compact_daily",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "gist", "compact"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_job = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="airnet",
        auto_remove=False,
        tty=True,
        mount_tmp_dir=False,
        command="/opt/spark/bin/spark-submit "
                "--class com.example.SparkCompactJob "
                "--master local[*] "
                "--driver-class-path /opt/postgresql-jdbc.jar "
                "--jars /opt/postgresql-jdbc.jar "
                "/opt/job/target/scala-2.12/spark-compact-job-assembly.jar",
        environment={
            "SPARK_HOME": "/opt/spark",
            "DATA_DIR": "/data/parquet",
            "TARGET_FILE_MB": "64",
            "GIST_URL": "https://gist.githubusercontent.com/oerasov/1905065dc6c0133267ec2a8167318399/raw/SmallFiles.scala",
            "SMALLFILES_COLS": "10",
            "SMALLFILES_ROWS": "4000000",
            "SMALLFILES_PARTITIONS": "30",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> run_job >> end
