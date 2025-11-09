FROM eclipse-temurin:8-jdk
# Если предпочтёшь Java 11 (Spark 3.4.x совместим): FROM eclipse-temurin:11-jdk

ARG SPARK_VERSION=3.4.2
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Базовые утилиты
RUN apt-get update && apt-get install -y curl gnupg ca-certificates bash && rm -rf /var/lib/apt/lists/*

# Установка sbt (через deb + auto deps)
RUN curl -fsSL https://repo.scala-sbt.org/scalasbt/debian/sbt-1.5.5.deb -o /tmp/sbt.deb \
 && dpkg -i /tmp/sbt.deb || true \
 && apt-get -f install -y \
 && rm /tmp/sbt.deb

# Spark дистрибутив
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Драйвер Postgres
ARG PG_JDBC_VER=42.7.4
RUN curl -fsSL -o /opt/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VER}.jar

WORKDIR /opt/job
COPY build.sbt ./build.sbt
COPY project ./project
COPY src ./src

# Сборка fat-jar
RUN sbt clean assembly

# Скрипт запуска
RUN bash -c 'cat > /opt/job/run.sh' <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

DATA_DIR=${DATA_DIR:-/data/parquet}
TARGET_FILE_MB=${TARGET_FILE_MB:-64}
GENERATE_TOTAL_MB=${GENERATE_TOTAL_MB:-300}
GIST_URL=${GIST_URL:-}
PG_HOST=${PG_HOST:-postgres}
PG_PORT=${PG_PORT:-5432}
PG_DB=${PG_DB:-airflow}
PG_USER=${PG_USER:-airflow}
PG_PASSWORD=${PG_PASSWORD:-airflow}

mkdir -p "$DATA_DIR"

"$SPARK_HOME/bin/spark-submit" \
  --class com.example.SparkCompactJob \
  --master local[*] \
  --driver-class-path /opt/postgresql-jdbc.jar \
  --jars /opt/postgresql-jdbc.jar \
  /opt/job/target/scala-2.12/spark-compact-job-assembly.jar \
  --data-dir "$DATA_DIR" \
  --target-file-mb "$TARGET_FILE_MB" \
  --generate-total-mb "$GENERATE_TOTAL_MB" \
  --gist-url "$GIST_URL" \
  --pg-host "$PG_HOST" --pg-port "$PG_PORT" --pg-db "$PG_DB" \
  --pg-user "$PG_USER" --pg-password "$PG_PASSWORD"

EOF

RUN chmod +x /opt/job/run.sh

ENTRYPOINT ["/bin/bash"]
