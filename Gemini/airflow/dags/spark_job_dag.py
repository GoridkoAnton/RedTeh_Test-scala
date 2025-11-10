import pendulum
from datetime import datetime
from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.param import Param

# Переменные окружения для Spark-задания (из .env)
# Airflow передаст их внутрь Docker-контейнера
DB_ENV_VARS = {
    "DB_URL": "{{ var.value.get('DB_URL', 'jdbc:postgresql://postgres:5432/airflow') }}",
    "DB_USER": "{{ var.value.get('DB_USER', 'airflow') }}",
    "DB_PASS": "{{ var.value.get('DB_PASS', 'airflow') }}",
}

# Имя Docker-образа, который мы создадим
# TODO: Замените 'my-spark-job:latest' на имя вашего образа после сборки
SPARK_JOB_IMAGE = "my-spark-job:latest"

with DAG(
    dag_id="daily_spark_compaction",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # Ежедневно в 14:00 UTC (Пункт 5)
    schedule_interval="0 14 * * *",
    catchup=False,
    tags=["spark", "docker", "compaction"],
) as dag:

    run_spark_job = DockerOperator(
        task_id="run_spark_compaction_job",
        image=SPARK_JOB_IMAGE,
        # Запускаем в той же Docker-сети, что и Airflow (из docker-compose)
        # 'host' - самый простой способ, но 'airflow_default' (по имени проекта) - безопаснее
        network_mode="host", # Либо <имя_проекта>_default, e.g. "airflow-solution_default"
        # Передаем переменные окружения для подключения к БД
        environment=DB_ENV_VARS,
        # Монтируем тот же том, который определен в docker-compose
        # <имя_тома>:<путь_в_контейнере>
        volumes=["shared-data:/opt/spark/data"],
        # Обязательно, чтобы Airflow мог запускать контейнеры
        docker_url="unix://var/run/docker.sock",
        # Автоматически удалять контейнер после завершения
        auto_remove=True,
        # Мы можем переопределить CMD из Dockerfile, если нужно
        # command="--class SparkCompactJob ...",
    )