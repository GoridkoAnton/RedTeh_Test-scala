FROM openjdk:8-jdk

# Spark 3.4.2 (Scala 2.12)
ARG SPARK_VERSION=3.4.2
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12.15
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Установим curl, scala, sbt
RUN apt-get update && apt-get install -y curl gnupg ca-certificates bash && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://downloads.lightbend.com/scala/2.12.15/scala-2.12.15.deb -o /tmp/scala.deb && dpkg -i /tmp/scala.deb && rm /tmp/scala.deb
RUN curl -fsSL https://repo.scala-sbt.org/scalasbt/debian/sbt-1.5.5.deb -o /tmp/sbt.deb && dpkg -i /tmp/sbt.deb || true && apt-get -f install -y && rm /tmp/sbt.deb

# Spark дистрибутив
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Постгрес драйвер для JDBC
ARG PG_JDBC_VER=42.7.4
RUN curl -fsSL -o /opt/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-${PG_JDBC_VER}.jar

WORKDIR /opt/job
COPY build.sbt ./build.sbt
COPY project ./project
COPY src ./src

# Соберём fat-jar
RUN sbt clean assembly

# Скрипт запуска
RUN bash -c 'cat > /opt/job/run.sh << "EOF"\n\
#!/usr/bin/env bash\n\
set -euo pipefail\n\
DATA_DIR=${DATA_DIR:-/data/parquet}\n\
TARGET_FILE_MB=${TARGET_FILE_MB:-64}\n\
GENERATE_TOTAL_MB=${GENERATE_TOTAL_MB:-300}\n\
GIST_URL=${GIST_URL:-}\n\
PG_HOST=${PG_HOST:-postgres}\n\
PG_PORT=${PG_PORT:-5432}\n\
PG_DB=${PG_DB:-airflow}\n\
PG_USER=${PG_USER:-airflow}\n\
PG_PASSWORD=${PG_PASSWORD:-airflow}\n\
mkdir -p \"$DATA_DIR\"\n\
# Запуск локального Spark job\n\
$SPARK_HOME/bin/spark-submit \\\n\
  --class com.example.SparkCompactJob \\\n\
  --master local[*] \\\n\
  --driver-class-path /opt/postgresql-jdbc.jar \\\n\
  --jars /opt/postgresql-jdbc.jar \\\n\
  /opt/job/target/scala-2.12/spark-compact-job-assembly.jar \\\n\
  --data-dir \"$DATA_DIR\" \\\n\
  --target-file-mb \"$TARGET_FILE_MB\" \\\n\
  --generate-total-mb \"$GENERATE_TOTAL_MB\" \\\n\
  --gist-url \"$GIST_URL\" \\\n\
  --pg-host \"$PG_HOST\" --pg-port \"$PG_PORT\" --pg-db \"$PG_DB\" \\\n\
  --pg-user \"$PG_USER\" --pg-password \"$PG_PASSWORD\"\n\
EOF\n\
chmod +x /opt/job/run.sh'

ENTRYPOINT ["/bin/bash"]
