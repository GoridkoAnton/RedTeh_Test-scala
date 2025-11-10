-- Этот скрипт будет выполнен автоматически при старте postgres
-- Мы используем ту же БД 'airflow', как указано в задаче.

CREATE TABLE IF NOT EXISTS public.spark_job_metadata (
    id SERIAL PRIMARY KEY,
    data_path VARCHAR(255) NOT NULL,
    number_of_files INT NOT NULL,
    average_files_size_mb FLOAT NOT NULL,
    dt TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Даем права пользователю 'airflow' на работу с этой таблицей
GRANT ALL PRIVILEGES ON TABLE public.spark_job_metadata TO airflow;
GRANT USAGE, SELECT ON SEQUENCE public.spark_job_metadata_id_seq TO airflow;