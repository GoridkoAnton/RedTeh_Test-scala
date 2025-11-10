# Базовый JDK 17 (совместим со Spark 3.5.x)
FROM eclipse-temurin:17-jdk-jammy

USER root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ---- Пакеты ----
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      curl ca-certificates bash coreutils procps tar unzip gnupg && \
    rm -rf /var/lib/apt/lists/*

# ---- Переменные окружения ----
ENV SPARK_VERSION=3.5.1 \
    HADOOP_PROFILE=hadoop3 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/usr/local/sbt/bin:/usr/local/bin:/usr/bin:/bin

# ---- Установка Apache Spark (предсобранный под hadoop3) ----
# Пробуем несколько зеркал, чтобы не упасть на 404 одного из них
RUN for U in \
      "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
      "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
      "https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz" \
    ; do \
      echo "Trying $U" && curl -fSL "$U" -o /tmp/spark.tgz && break || echo "Mirror failed: $U"; \
    done && \
    test -s /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv "/opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}" "${SPARK_HOME}" && \
    rm -f /tmp/spark.tgz

# ---- JDBC-драйвер PostgreSQL ----
ENV PG_JDBC_VER=42.7.3
RUN curl -fsSL "https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VER}.jar" -o /opt/postgresql-jdbc.jar

# ---- Установка sbt (tgz-вариант) ----
ENV SBT_VERSION=1.9.9
RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" -o /tmp/sbt.tgz && \
    mkdir -p /usr/local/sbt && \
    tar -xzf /tmp/sbt.tgz -C /usr/local/sbt --strip-components=1 && \
    ln -sf /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    rm -f /tmp/sbt.tgz

# ---- Рабочая директория и проект ----
WORKDIR /opt/job
COPY . /opt/job

# ---- ENV по умолчанию (можно переопределять из Airflow) ----
ENV DATA_DIR=/data/parquet \
    TARGET_FILE_MB=64 \
    SMALLFILES_COLS=10 \
    SMALLFILES_ROWS=4000000 \
    SMALLFILES_PARTITIONS=30 \
    GIST_URL=https://gist.githubusercontent.com/oerasov/1905065dc6c0133267ec2a8167318399/raw/SmallFiles.scala

# ---- Сборка JAR ----
RUN sbt -v -Dsbt.log.noformat=true clean assembly

# ---- Скрипт запуска ----
RUN cat > /opt/job/run <<'EOF' && chmod +x /opt/job/run
#!/usr/bin/env bash
set -euo pipefail

echo "=== SparkCompactJob starting ==="
echo "SPARK_HOME=${SPARK_HOME}"
echo "DATA_DIR=${DATA_DIR}"
echo "TARGET_FILE_MB=${TARGET_FILE_MB}"
echo "GIST_URL=${GIST_URL}"

"${SPARK_HOME}/bin/spark-submit" \
  --class com.example.SparkCompactJob \
  --master local[*] \
  --driver-class-path /opt/postgresql-jdbc.jar \
  --jars /opt/postgresql-jdbc.jar \
  /opt/job/target/scala-2.12/spark-compact-job-assembly.jar \
  --data-dir="${DATA_DIR}" \
  --target-file-mb="${TARGET_FILE_MB}"
EOF

ENTRYPOINT ["/opt/job/run"]
