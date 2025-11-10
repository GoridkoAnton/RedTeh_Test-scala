from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from docker.types import Mount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='compact_parquet_and_register',
    default_args=default_args,
    schedule_interval='0 14 * * *',  # 14:00 UTC daily
    catchup=False,
    tags=['spark','parquet','postgres'],
) as dag:

    run_spark_compact = DockerOperator(
        task_id='run_spark_compact',
        image='compact-parquet:latest',  # соберите локально и пометьте этим тегом
        api_version='auto',
        auto_remove=True,
        command='compact /data/parquet 50 jdbc:postgresql://postgres:5432/airflow airflow airflow',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        mounts=[Mount(target="/data", source="PATH_TO_YOUR_LOCAL_DATA_DIR", type="bind")],
    )

    run_spark_compact