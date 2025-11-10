FROM bitnami/spark:3.5.1

USER root

# Устанавливаем необходимые пакеты и Java
RUN apt-get update && apt-get install -y curl openjdk-17-jdk scala && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /opt/job

# Копируем проект
COPY . /opt/job

# Загружаем JDBC-драйвер PostgreSQL
RUN curl -L -o /opt/postgresql-jdbc.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Устанавливаем переменные окружения
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV DATA_DIR=/data/parquet
ENV TARGET_FILE_MB=64
ENV SMALLFILES_COLS=10
ENV SMALLFILES_ROWS=4000000
ENV SMALLFILES_PARTITIONS=30
ENV GIST_URL=https://gist.githubusercontent.com/oerasov/1905065dc6c0133267ec2a8167318399/raw/SmallFiles.scala

# Сборка Scala-приложения (если используется sbt)
RUN if [ -f "build.sbt" ]; then \
      curl -L -o sbt.tgz https://github.com/sbt/sbt/releases/download/v1.9.7/sbt-1.9.7.tgz && \
      tar -xzf sbt.tgz -C /usr/local && \
      ln -s /usr/local/sbt/bin/sbt /usr/bin/sbt && \
      sbt clean assembly; \
    else \
      echo "build.sbt не найден — пропускаем сборку"; \
    fi

# Команда по умолчанию (используется Airflow)
CMD ["/bin/bash", "-c", "echo Spark job image ready"]
