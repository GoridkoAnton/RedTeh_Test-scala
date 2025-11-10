# Базовый образ Java 8 (для совместимости со Spark 3.1.x)
FROM openjdk:8-jdk

# Метаданные
LABEL maintainer="RedTeh DevOps" \
      description="Spark Compact Job with Gist Loader"

# ------------------------------------------------------------------------------
# 1. Установка системных зависимостей: curl, bash, scala
# ------------------------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        bash \
        scala \
        unzip \
        procps && \
    rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------------------------------
# 2. Установка Apache Spark
# ------------------------------------------------------------------------------
ENV SPARK_VERSION=3.1.3 \
    HADOOP_VERSION=3.2 \
    SPARK_HOME=/opt/spark \
    PATH=$PATH:/opt/spark/bin

RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz && \
    mkdir -p /opt && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# ------------------------------------------------------------------------------
# 3. Добавление PostgreSQL JDBC-драйвера
# ------------------------------------------------------------------------------
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o /opt/postgresql-jdbc.jar

# ------------------------------------------------------------------------------
# 4. Копируем исходники и собираем Scala-проект
# ------------------------------------------------------------------------------
WORKDIR /opt/job

COPY ./spark-job /opt/job

RUN ./sbt clean assembly

# ------------------------------------------------------------------------------
# 5. Подготавливаем скрипт запуска
# ------------------------------------------------------------------------------
RUN bash -c 'cat > /opt/job/run << "EOF" \
#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=${DATA_DIR:-/data/parquet}
TARGET_FILE_MB=${TARGET_FILE_MB:-64}
GIST_URL=${GIST_URL:-}

echo "🚀 Starting SparkCompactJob"
echo "DATA_DIR=$DATA_DIR"
echo "TARGET_FILE_MB=$TARGET_FILE_MB"
echo "GIST_URL=$GIST_URL"

$SPARK_HOME/bin/spark-submit \
  --class com.example.SparkCompactJob \
  --master local[*] \
  --driver-class-path /opt/postgresql-jdbc.jar \
  --jars /opt/postgresql-jdbc.jar \
  /opt/job/target/scala-2.12/spark-compact-job-assembly.jar \
  --data-dir=$DATA_DIR \
  --target-file-mb=$TARGET_FILE_MB
EOF' && chmod +x /opt/job/run

# ------------------------------------------------------------------------------
# 6. Указываем рабочую директорию и точку входа
# ------------------------------------------------------------------------------
WORKDIR /opt/job
ENTRYPOINT ["/opt/job/run"]
