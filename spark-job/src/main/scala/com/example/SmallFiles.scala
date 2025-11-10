package com.example

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, DoubleType}
import scala.util.Random

object SmallFiles {
  // сигнатура как в gist: generateDf(numberOfCols, numberOfRows)
  def generateDf(spark: SparkSession, numberOfCols: Int, numberOfRows: Int): DataFrame = {
    val schema = StructType(
      Array.fill(numberOfCols)(
        StructField(Random.alphanumeric.dropWhile(_.isDigit).take(10).mkString, DoubleType, nullable = false)
      )
    )
    val rdd = spark.sparkContext.parallelize(
      (0 until numberOfRows).map(_ => Row.fromSeq(Seq.fill(numberOfCols)(Random.nextDouble())))
    )
    spark.createDataFrame(rdd, schema)
  }

  // сигнатура как в gist: generateSmallFiles(spark, smallFilesPath)
  def generateSmallFiles(spark: SparkSession, smallFilesPath: String): Unit = {
    val cols       = sys.env.get("SMALLFILES_COLS").map(_.toInt).getOrElse(10)
    val rows       = sys.env.get("SMALLFILES_ROWS").map(_.toInt).getOrElse(4000000)
    val partitions = sys.env.get("SMALLFILES_PARTITIONS").map(_.toInt).getOrElse(30)

    generateDf(spark, cols, rows)
      .repartition(partitions)
      .write
      .mode(SaveMode.Append)
      .parquet(smallFilesPath)
  }
}
