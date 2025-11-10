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
    schedule="0 14 * * *",
    catchup=False,
    tags=["spark", "compact"],
) as dag:

    begin = EmptyOperator(task_id="begin")

    run_compact = DockerOperator(
        task_id="run_spark_compact",
        image="spark-compact-job:latest",
        api_version="auto",
        auto_remove=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",

        mounts=[Mount(source="shared_data", target="/data", type="volume")],

        environment={
            "DATA_DIR": "/data/parquet",
            "TARGET_FILE_MB": "64",
            "GENERATE_TOTAL_MB": "300",
            "GIST_URL": "",
            "PG_HOST": "postgres",
            "PG_PORT": "5432",
            "PG_DB": "airflow",
            "PG_USER": "airflow",
            "PG_PASSWORD": "airflow",
        },

        tty=True,
        command="/opt/job/run",  # теперь без .sh
        mount_tmp_dir=False,
    )

    end = EmptyOperator(task_id="end")

    begin >> run_compact >> end
