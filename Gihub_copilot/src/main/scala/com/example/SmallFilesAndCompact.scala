package com.example

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.util.Properties
import scala.util.Random

object SmallFilesAndCompact {

  // Ваша функция генерации DataFrame
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

  // Ваша функция генерации множества parquet-файлов (append)
  def generateSmallFiles(spark: SparkSession, smallFilesPath: String): Unit = {
    generateDf(spark, 10, 4000000)
      .repartition(30)
      .write
      .mode(SaveMode.Append)
      .parquet(smallFilesPath)
  }

  // Функция compact + запись метаданных в PostgreSQL
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

    metaDf.write.mode("append").jdbc(jdbcUrl, "parquet_catalog", props)
  }

  // main: режимы generate / compact
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
          generateSmallFiles(spark, parquetPath)
        case "compact" =>
          // ожидаем args: compact <parquetPath> <targetSizeMb> <jdbcUrl> <jdbcUser> <jdbcPass>
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
      spark.stop()
    }
  }
}