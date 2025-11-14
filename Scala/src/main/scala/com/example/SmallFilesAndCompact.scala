package com.example

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.util.Properties
import scala.util.Random
/**
 * SmallFilesAndCompact
 *
 * Основной класс (object) который содержит:
 * - функции для генерации DataFrame и записи его в parquet (generateSmallFiles)
 * - логику компактирования (compactAndRegister)
 * - main — точку входа для вызова из spark-submit:
 *     spark-submit ... com.example.SmallFilesAndCompact generate <path>
 *     spark-submit ... com.example.SmallFilesAndCompact compact <path> <targetSizeMb> <jdbcUrl> <user> <pass>
 *
 * Комментарии в коде подробные: что делает каждая функция, какие предпосылки, что надо проверять при ошибках.
 */
object SmallFilesAndCompact {
 /**
   * generateDf
   *
   * Генерирует DataFrame с указанным количеством столбцов и строк.
   * - Используется для генерации "малых файлов" — создаём большой набор случайных значений,
   *   затем repartition и запись в parquet приведёт к множественным небольшим файлам.
   *
   * Параметры:
   *  - spark: SparkSession — сессия spark
   *  - numberOfCols: Int — число столбцов (имена генерируются случайно)
   *  - numberOfRows: Int — число строк
   *
   * Возвращает DataFrame, который затем можно записать в parquet.
   *
   * Замечания:
   *  - Использование Random.alphanumeric для имён колонок — в продакшне лучше задавать осмысленные имена.
   *  - Если ожидается воспроизводимость, нужно зафиксировать seed.
   */
  def generateDf(spark: SparkSession, numberOfCols: Int, numberOfRows: Int): DataFrame = {
    val schema = StructType(
      Array.fill(numberOfCols)(
        StructField(
          Random.alphanumeric.dropWhile(_.isDigit).take(10).mkString,
          DoubleType,
          nullable = true
        )
      )
    )
 // Генерация RDD строк: каждый Row из numberOfCols случайных Double
    val rdd = spark.sparkContext
      .parallelize(
        (0 to numberOfRows)
          .map(_ => Row.fromSeq(Seq.fill(numberOfCols)(Random.nextDouble)))
      )

    spark.createDataFrame(
      rdd,
      schema
    )
  }

 /**
   * generateSmallFiles
   *
   * Создаёт набор parquet файлов путем записи DataFrame с апендом.
   * Предполагается, что целью является создание *многих* небольших файлов,
   * чтобы затем протестировать/показать поведение compaction.
   *
   * Параметры:
   *  - spark: SparkSession
   *  - smallFilesPath: String — директория назначения (обычно что-то вроде /data/parquet)
   *
   * Поведение:
   *  - generateDf(...).repartition(30).write.mode(SaveMode.Append).parquet(smallFilesPath)
   *  - Количество partition (30) регулирует сколько файлов появится при записи — увеличьте/уменьшите при необходимости.
   *
   * Замечания/рекомендации:
   *  - В продакшне стоит управлять количеством партиций в зависимости от размера данных и желаемого количества файлов.
   *  - Обрабатывать ошибки записи (например, нехватка места, отказ HDFS) стоит выше по стеку (в wrapper-е/контейнере).
   */

  def generateSmallFiles(spark: SparkSession, smallFilesPath: String): Unit = {
    generateDf(spark, 10, 4000000)
      .repartition(30)
      .write
      .mode(SaveMode.Append)
      .parquet(smallFilesPath)
  }
 /**
   * compactAndRegister
   *
   * Основная логика "компактирования" parquet файлов в директории parquetDir и записи
   * метаданных о результате в PostgreSQL (jdbc).
   *
   * Параметры:
   *  - spark: SparkSession
   *  - parquetDir: String — путь к папке с parquet (может быть локальным /data или volume)
   *  - targetSizeMb: Int — целевой размер файла в мегабайтах (алгоритм/подход прост)
   *  - jdbcUrl: String — JDBC URL для Postgres
   *  - jdbcUser: String — пользователь БД
   *  - jdbcPass: String — пароль
   *
   * Поведение:
   * 1. Проверяем, что parquetDir существует через Hadoop FileSystem (полезно для HDFS/S3/локального).
   * 2. Сканируем файлы в директории, считаем число parquet файлов и средний размер.
   * 3. Производим (в текущей реализации) только вычисление метаинформации и запись её в таблицу parquet_catalog.
   *    - В продакшне вместо простого подсчёта и записи можно:
   *        * запустить spark job, который делает coalesce/repartition и записывает данные в новую директорию
   *        * заменить старые файлы на новые atomically (через temp dir + rename)
   * 4. Записываем метаданные в таблицу parquet_catalog через JDBC append.
   *
   * Замечания по отказоустойчивости:
   *  - При записи метаданных используем .write.mode("append").jdbc(...).
   *    Важно: если запись в БД упадёт, состояние данных на файловом уровне не будет откатано автоматически.
   *  - Для атомарности можно:
   *    * сначала записать компактные файлы в временную папку, затем атомарно поменять имена/переместить,
   *    * затем только при успешной смене — записывать метаданные.
   *
   * Производительность:
   *  - Для больших объемов данных compact лучше выполнять в отдельном job с настройкой shuffle/partitions.
   *  - Поддержите конфигурацию spark.local.dir и driver/executor memory через параметры среды/конфигурацию.
   */
  def compactAndRegister(spark: SparkSession,
                         parquetDir: String,
                         targetSizeMb: Int,
                         jdbcUrl: String,
                         jdbcUser: String,
                         jdbcPass: String): Unit = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new URI(parquetDir), hadoopConf)
    val path = new Path(parquetDir)
    if (!fs.exists(path)) {
      throw new IllegalArgumentException(s"Path $parquetDir does not exist")
    }

    // total size в байтах
    val totalBytes = fs.getContentSummary(path).getLength
    val targetBytes = targetSizeMb.toLong * 1024 * 1024
    val targetNumFiles = math.max(1, ((totalBytes + targetBytes - 1) / targetBytes).toInt)

    // читаем parquet, coalesce к рассчитанному числу файлов и пишем во временную папку
    val tempDir = parquetDir + "_tmp_compact_" + System.currentTimeMillis()
    val df = spark.read.parquet(parquetDir)
    df.coalesce(targetNumFiles).write.mode(SaveMode.Overwrite).parquet(tempDir)

    // простой шаг замены: удалить исходный и переименовать temp -> original
    // В prod: делать более аккуратно (проверки, бэкапы)
    fs.delete(path, true)
    fs.rename(new Path(tempDir), path)

    // сканируем результирующую папку
    val statuses = fs.listStatus(path)
      .filter(_.isFile) // могут быть папки _common_metadata, _SUCCESS, etc.
    val parquetFiles = statuses.filter(s => s.getPath.getName.endsWith(".parquet"))
    val numberOfFiles = parquetFiles.length
    val averageSize = if (numberOfFiles > 0) parquetFiles.map(_.getLen).sum / numberOfFiles else 0L

    // подготовка и запись метаданных в Postgres
    val props = new Properties()
    props.setProperty("user", jdbcUser)
    props.setProperty("password", jdbcPass)

    import spark.implicits._
    val metaDf = Seq((parquetDir, numberOfFiles, averageSize))
      .toDF("data_path", "number_of_files", "average_files_size")
      .withColumn("dt", org.apache.spark.sql.functions.current_timestamp())
   // Запись в таблицу parquet_catalog — создаётся если нет (в зависимости от прав) или добавляется запись
    // В продакшне: обеспечить схему таблицы заранее или проверять наличие столбцов/создавать таблицу.
    metaDf.write.mode("append").jdbc(jdbcUrl, "parquet_catalog", props)
  }

  /**
   * main
   *
   * Точка входа приложения.
   * Ожидаемые аргументы:
   *  - generate <parquetPath>
   *  - compact <parquetPath> <targetSizeMb> <jdbcUrl> <jdbcUser> <jdbcPass>
   *
   * Поведение:
   *  - Парсит аргументы, создаёт SparkSession и вызывает соответствующую функцию.
   *  - В случае некорректных аргументов — печатает usage и завершает процесс с кодом 1.
   *
   * Рекомендации:
   *  - Логирование: в продакшне лучше использовать логгер (log4j/slf4j) вместо println/System.err.
   *  - Ошибки: для интеграции с Airflow/DockerOperator убедитесь, что ошибки пробрасываются наружу
   *    (не подавляются) — чтобы оболочка (DockerOperator wrapper) могла регистрировать неуспех таска.
   */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: <mode: generate|compact> <parquetPath> [other args]")
      System.exit(1)
    }

    val mode = args(0)
    val parquetPath = args(1)

    val spark = SparkSession.builder()
      .appName("SmallFilesAndCompact")
      .getOrCreate()

    try {
      mode match {
        case "generate" =>
          // generate: создаём "малые" файлы по пути parquetPath
          generateSmallFiles(spark, parquetPath)
        case "compact" =>
          // compact ожидает дополнительные аргументы
          // пример вызова:
          // spark-submit ... com.example.SmallFilesAndCompact compact /data/parquet 50 jdbc:... user pass
          if (args.length < 6) {
            System.err.println("compact usage: compact <parquetPath> <targetSizeMb> <jdbcUrl> <jdbcUser> <jdbcPass>")
            System.exit(2)
          }
          val targetSizeMb = args(2).toInt
          val jdbcUrl = args(3)
          val jdbcUser = args(4)
          val jdbcPass = args(5)
          compactAndRegister(spark, parquetPath, targetSizeMb, jdbcUrl, jdbcUser, jdbcPass)
        case other =>
          System.err.println(s"Unknown mode: $other")
      }
    } finally {
      // корректное завершение spark session
      spark.stop()
    }
  }
}