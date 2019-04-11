package com.somefruit.domain.statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zazhigin on 10.04.19.
  */
object MedianDurationStatistics {

  /**
    * Starts median duration statistics task.
    *
    * @param inputFile The input CSV file.
    * @param outputDir The output directory where result CSV file stored.
    */
  def start(inputFile: String, outputDir: String): Unit = {
    val conf = new SparkConf().setAppName("MedianDurationStatistics").setMaster("local[*]")
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
      val sessionDF = session.sql("SELECT category, sessionId, sessionStartTime, sessionEndTime"+
        " FROM sessions GROUP BY category, sessionId, sessionStartTime, sessionEndTime")

      // Calculate session duration and filter out zero duration (zero means single event within whole session)
      val duration =
        unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime"))
      val durationDF = sessionDF.withColumn("duration", duration).filter("duration > 0")

      // Aggregate median session duration
      val durationWindow = Window.partitionBy("category").orderBy("duration")
      val index = count("duration").over(durationWindow)
      val indexDF = durationDF.withColumn("index", index)
      val maxWindow = Window.partitionBy("category").orderBy(col("index").desc)
      val maxDF = indexDF.withColumn("max", first("index").over(maxWindow))
      val odd = when(col("max")%2>0, (col("max")-1)/2+1).otherwise(0)
      val even1 = when(col("max")%2===0, col("max")/2).otherwise(0)
      val even2 = when(col("max")%2===0, col("max")/2+1).otherwise(0)
      val median = when(col("odd")===col("index"), col("duration"))
        .when(col("even1")===col("index"), col("duration")/2)
        .when(col("even2")===col("index"), col("duration")/2)
        .otherwise(0)
      val medianDF = maxDF
        .withColumn("odd", odd.cast("Int"))
        .withColumn("even1", even1.cast("Int"))
        .withColumn("even2", even2.cast("Int"))
        .withColumn("median", median)

      // Calculate median session duration
      medianDF.createOrReplaceTempView("sessions")
      val outputDF = session.sql("SELECT category, SUM(median) AS median FROM sessions GROUP BY category")

      outputDF.coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .save(outputDir+"/medianDurationStatistics")
    }
    finally {
      session.stop()
      sc.stop()
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: MedianDurationStatistics <input_file> <output_dir>")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputDir = args(1)
    start(inputFile, outputDir)
  }
}
