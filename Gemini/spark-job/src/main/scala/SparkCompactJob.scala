import org.apache.spark.sql.{SaveMode, SparkSession}
import java.nio.file.{Files, Paths}
import java.util.Properties
import java.sql.Timestamp
import java.time.Instant

object SparkCompactJob {

  // =================================================================================
  // Код из Gist (Пункт 4.a)
  // =================================================================================
  def generateSmallFiles(spark: SparkSession, outputPath: String): Unit = {
    import spark.implicits._

    // Удаляем директорию, если она существует
    val path = Paths.get(outputPath)
    if (Files.exists(path)) {
      Files.walk(path)
        .sorted(java.util.Comparator.reverseOrder())
        .map(_.toFile)
        .forEach(f => f.delete())
    }

    val df = spark.range(0, 1000 * 1000).toDF("id")
      .withColumn("data", ($"id" * 10).cast("string"))

    // Создаем множество мелких файлов
    df.repartition(200) // Генерируем 200 Parquet файлов
      .write
      .parquet(outputPath)
  }

  // =================================================================================
  // Функция для реализации (Пункт 4.b)
  // =================================================================================
  /**
   * Сжимает мелкие parquet-файлы в более крупные.
   *
   * @param spark SparkSession
   * @param inputDir Директория с мелкими файлами
   * @param outputDir Директория для сжатых файлов
   * @param targetFileSizeMB Ожидаемый размер файла в МБ
   */
  def compact(spark: SparkSession, inputDir: String, outputDir: String, targetFileSizeMB: Int): Unit = {
    println(s"Компактизация: $inputDir -> $outputDir (цель: ${targetFileSizeMB}MB)")
    val df = spark.read.parquet(inputDir)

    // TODO: Улучшить эту логику.
    // Это очень упрощенный расчет. В идеале нужно оценить размер датафрейма в памяти
    // и разделить на targetFileSizeMB, но для этого нужно сначала кэшировать df.
    // Для простоты пока делим на 2 партиции.
    val numPartitions = 2 // (df.count() * [байт на строку] / (targetFileSizeMB * 1024 * 1024)).toInt

    df.repartition(numPartitions)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputDir)
  }

  // =================================================================================
  // Логика записи метаданных (Пункт 4.c)
  // =================================================================================
  /**
   * Сканирует каталог и записывает метаданные в PostgreSQL.
   */
  def writeMetadata(spark: SparkSession, dirPath: String, dbProps: Properties, dbUrl: String): Unit = {
    println(s"Сбор метаданных для: $dirPath")

    // Используем Hadoop API (через SparkContext) для работы с файловой системой
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val path = new org.apache.hadoop.fs.Path(dirPath)

    val files = fs.listStatus(path)
      .filter(f => f.isFile && f.getPath.getName.endsWith(".parquet"))

    val numFiles = files.length
    val totalSize = files.map(_.getLen).sum
    val avgSizeMB = if (numFiles > 0) (totalSize.toDouble / numFiles) / (1024 * 1024) else 0.0

    println(s"Найдено файлов: $numFiles")
    println(f"Средний размер: $avgSizeMB%.2f MB")

    import spark.implicits._

    // Создаем DataFrame для записи в БД
    val metadataDF = Seq(
      (dirPath, numFiles, avgSizeMB, new Timestamp(Instant.now().toEpochMilli))
    ).toDF("data_path", "number_of_files", "average_files_size_mb", "dt")

    metadataDF.write
      .mode(SaveMode.Append)
      .jdbc(dbUrl, "public.spark_job_metadata", dbProps)

    println("Метаданные успешно записаны в PostgreSQL.")
  }

  // =================================================================================
  // Главная функция
  // =================================================================================
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Compact Job")
      .master("local[*]") // Работаем в локальном режиме
      .getOrCreate()

    // Пути внутри тома 'shared-data', который мы монтировали в docker-compose
    val dataRoot = "/opt/spark/data"
    val smallFilesDir = s"$dataRoot/small_files"
    val compactedFilesDir = s"$dataRoot/compacted_files"
    val targetFileSize = 100 // Целевой размер файла (например, 100МБ)

    // Параметры подключения к БД (ожидаем их из переменных окружения)
    val dbUrl = sys.env.getOrElse("DB_URL", "jdbc:postgresql://postgres:5432/airflow")
    val dbProps = new Properties()
    dbProps.setProperty("user", sys.env.getOrElse("DB_USER", "airflow"))
    dbProps.setProperty("password", sys.env.getOrElse("DB_PASS", "airflow"))
    dbProps.setProperty("driver", "org.postgresql.Driver")

    try {
      // 1. Генерируем файлы (Пункт 4.a)
      generateSmallFiles(spark, smallFilesDir)

      // 2. Выполняем компактизацию (Пункт 4.b)
      compact(spark, smallFilesDir, compactedFilesDir, targetFileSize)

      // 3. Записываем метаданные (Пункт 4.c)
      writeMetadata(spark, compactedFilesDir, dbProps, dbUrl)

    } catch {
      case e: Exception =>
        println(s"Ошибка в Spark-задании: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}