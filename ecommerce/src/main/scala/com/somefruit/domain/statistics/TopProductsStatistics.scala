package com.somefruit.domain.statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zazhigin on 10.04.19.
  */
object TopProductsStatistics {

  /**
    * Starts top products ranked by time spent statistics task.
    *
    * @param inputFile The input CSV file.
    * @param outputDir The output directory where result CSV file stored.
    * @param topNumber The top number for products to rank.
    */
  def start(inputFile: String, outputDir: String, topNumber: Int): Unit = {
    val conf = new SparkConf().setAppName("TopProductsStatistics").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder
      .config(conf = conf)
      .getOrCreate()

    try {
      val df = session.read
        .option("header", "true")
        .csv(inputFile)

      // Select groups by categories & sessions
      df.createOrReplaceTempView("sessions")
      val sessionDF = session.sql("SELECT category, product, sessionId, sessionStartTime, sessionEndTime"+
        " FROM sessions GROUP BY category, product, sessionId, sessionStartTime, sessionEndTime")

      // Calculate session duration by particular product
      val duration =
        unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime"))
      val durationDF = sessionDF.withColumn("duration", duration)

      // Filter maximum duration by category / product
      val durationWindow = Window.partitionBy("category", "product")
        .orderBy(desc("duration"))
      val max = first("duration").over(durationWindow)
      val filterDF = durationDF.withColumn("max", max)
        .filter("duration == max")
        .drop("max")

      // Rank category products by descending duration with topNumber limit
      val categoryWindow = Window.partitionBy("category")
        .orderBy(desc("duration"))
      val rankDF = filterDF.withColumn("rank", rank.over(categoryWindow))
        .where(col("rank") <= topNumber)

      // Select category products by time spent
      rankDF.createOrReplaceTempView("sessions")
      val outputDF = session.sql("SELECT category, product, duration FROM sessions"+
        " ORDER BY category, duration DESC, product")

      outputDF.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .save(outputDir+"/topProductsStatistics")
    }
    finally {
      session.stop()
      sc.stop()
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: TopProductsStatistics <input_file> <output_dir> <top_number>")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputDir = args(1)
    val topNumber = args(2).toInt
    start(inputFile, outputDir, topNumber)
  }
}
