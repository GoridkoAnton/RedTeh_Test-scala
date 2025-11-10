# Используем официальный образ Spark 3.3.0 со Scala 2.12 и Java 8
# Это избавляет нас от ручной сборки sbt и установки Spark
FROM bitnami/spark:3.3.0

# Устанавливаем драйвер PostgreSQL
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpostgresql-jdbc-java \
    && rm -rf /var/lib/apt/lists/*
USER 1001

# Копируем JDBC драйвер в папку, которую Spark будет использовать
# (Это может зависеть от версии образа, /opt/bitnami/spark/jars - стандартный путь для bitnami)
USER root
RUN ln -s /usr/share/java/postgresql-jdbc4.jar /opt/bitnami/spark/jars/postgresql-jdbc.jar
USER 1001

# Устанавливаем sbt для сборки
USER root
RUN curl -L -o sbt-1.5.5.deb https://github.com/sbt/sbt/releases/download/v1.5.5/sbt-1.5.5.deb && \
    dpkg -i sbt-1.5.5.deb && \
    rm sbt-1.5.5.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER 1001

# Устанавливаем рабочую директорию
WORKDIR /opt/spark/job

# Копируем sbt-проект
COPY build.sbt .
COPY src ./src

# Собираем JAR
RUN sbt package

# Директория для данных, куда мы монтируем том
ENV SPARK_DATA_DIR=/opt/spark/data

# Команда для запуска. Мы будем передавать параметры через spark-submit
ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit"]

CMD ["--class", "SparkCompactJob", "--master", "local[*]", "./target/scala-2.12/spark-compact-job_2.12-0.1.0-SNAPSHOT.jar"]