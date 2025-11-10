package com.example

import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.sys.process._
import java.io.File

object SparkCompactJob {
  def main(args: Array[String]): Unit = {
    val spark    = SparkSession.builder().appName("SparkCompactJob").getOrCreate()
    val gistUrl  = sys.env.getOrElse("GIST_URL", "")
    val dataDir  = sys.env.getOrElse("DATA_DIR", "/data/parquet")
    val targetMb = sys.env.getOrElse("TARGET_FILE_MB", "64").toInt

    try {
      if (gistUrl.nonEmpty) {
        println(s"🔗 Loading external generator from Gist: $gistUrl")
        val tmpFile = new File("/tmp/SmallFiles.scala")
        Seq("curl", "-fsSL", gistUrl, "-o", tmpFile.getAbsolutePath).!
        println("✅ Gist downloaded, executing in spark-shell ...")
        Seq("/opt/spark/bin/spark-shell", "-i", tmpFile.getAbsolutePath).!
      } else {
        println("ℹ️ Using built-in generator SmallFiles.generateSmallFiles")
        SmallFiles.generateSmallFiles(spark, dataDir)
      }
      compactToTargetSize(spark, dataDir, targetMb)
    } finally {
      spark.stop()
    }
  }

  private def compactToTargetSize(spark: SparkSession, path: String, targetMb: Int): Unit = {
    val df = spark.read.parquet(path)
    val bytesPerRow   = 16
    val rowsPerTarget = math.max((targetMb.toLong * 1024 * 1024) / bytesPerRow, 1).toInt
    val totalRows     = df.count()
    val targetFiles   = math.max(math.ceil(totalRows.toDouble / rowsPerTarget).toInt, 1)

    println(s"Compacting ${totalRows} rows into ~${targetFiles} files of ${targetMb}MB each")
    df.coalesce(targetFiles)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(path.stripSuffix("/") + "_compact")
  }
}
