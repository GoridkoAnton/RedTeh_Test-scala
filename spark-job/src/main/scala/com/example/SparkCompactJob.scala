package com.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import java.nio.file.{Files, Paths}
import scala.sys.process._

object SparkCompactJob {

  case class Args(
    dataDir: String = "/data/parquet",
    targetFileMb: Int = 64,
    generateTotalMb: Int = 300,
    gistUrl: String = "",
    pgHost: String = "postgres",
    pgPort: String = "5432",
    pgDb: String = "airflow",
    pgUser: String = "airflow",
    pgPassword: String = "airflow"
  )

  def main(raw: Array[String]): Unit = {
    val p = parseArgs(raw)
    val spark = SparkSession.builder().appName("SparkCompactJob").getOrCreate()
    import spark.implicits._

    // 1) Генерация множества маленьких parquet (~ generateTotalMb)
    generateSmallFiles(spark, p)

    // 2) Компактизация в файлы около targetFileMb
    val input = p.dataDir + "/input"
    val output = p.dataDir + "/compact"
    val df = spark.read.parquet(input)

    val totalSizeBytes = java.nio.file.Files.walk(Paths.get(input))
      .filter(Files.isRegularFile(_)).mapToLong(p => Files.size(p)).sum()

    val partSize = Math.max(p.targetFileMb, 1) * 1024L * 1024L
    val targetParts = Math.max(1, Math.ceil(totalSizeBytes.toDouble / partSize.toDouble).toInt)

    df.repartition(targetParts)
      .write.mode(SaveMode.Overwrite).parquet(output)

    // 3) Подсчёт метрик и запись в Postgres
    val sizes = java.nio.file.Files.walk(Paths.get(output))
      .filter(Files.isRegularFile(_)).toArray.map(p => Files.size(p.asInstanceOf[java.nio.file.Path]))
      .map(_.toDouble / (1024D * 1024D))

    val numberOfFiles = sizes.length
    val avgMb = if (sizes.isEmpty) 0.0 else sizes.sum / numberOfFiles

    val jdbcUrl = s"jdbc:postgresql://${p.pgHost}:${p.pgPort}/${p.pgDb}"
    val metaDf = Seq(
      (output, numberOfFiles, BigDecimal(avgMb).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble)
    ).toDF("data_path", "number_of_files", "average_file_mb")

    metaDf.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "public.spark_compact_metadata")
      .option("user", p.pgUser)
      .option("password", p.pgPassword)
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

  private def generateSmallFiles(spark: org.apache.spark.sql.SparkSession, p: Args): Unit = {
    import spark.implicits._
    val input = p.dataDir + "/input"
    // Если указан gist — можно скачать и выполнить его (как отдельный шаг, если он — Scala script)
    if (p.gistUrl.nonEmpty) {
      try {
        val script = "/tmp/generateSmallFiles.scala"
        s"curl -fsSL ${p.gistUrl} -o $script".!!
        // Пример: spark-shell -i script (при необходимости можно адаптировать под формат gista)
        // Здесь оставим генерацию своим кодом на случай несовместимости:
      } catch {
        case _: Throwable => // fallback ниже
      }
    }
    // Fallback: генерируем синтетический датасет, который при записи даст ~p.generateTotalMb
    val rows = p.generateTotalMb * 1024 * 10 // грубая прикидка количества строк
    val rnd = new scala.util.Random(42)
    val df = (0 until rows).map { i =>
      (i, rnd.alphanumeric.take(128).mkString, rnd.nextInt())
    }.toDF("id","payload","n")

    // дробим на множество маленьких файлов (много партиций)
    df.repartition(200)
      .write.mode(SaveMode.Overwrite).parquet(input)
  }

  private def parseArgs(args: Array[String]): Args = {
    def next(i: Int) = args(i + 1)
    var p = Args()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--data-dir" => p = p.copy(dataDir = next(i)); i += 1
        case "--target-file-mb" => p = p.copy(targetFileMb = next(i).toInt); i += 1
        case "--generate-total-mb" => p = p.copy(generateTotalMb = next(i).toInt); i += 1
        case "--gist-url" => p = p.copy(gistUrl = next(i)); i += 1
        case "--pg-host" => p = p.copy(pgHost = next(i)); i += 1
        case "--pg-port" => p = p.copy(pgPort = next(i)); i += 1
        case "--pg-db" => p = p.copy(pgDb = next(i)); i += 1
        case "--pg-user" => p = p.copy(pgUser = next(i)); i += 1
        case "--pg-password" => p = p.copy(pgPassword = next(i)); i += 1
        case other => throw new IllegalArgumentException(s"Unknown arg: $other")
      }
      i += 1
    }
    p
  }
}
