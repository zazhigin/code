package com.somefruit.domain.sessions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, first, lag, sum, when}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zazhigin on 10.04.19.
  */
object WindowProductSessions {

  /**
    * Starts window product sessions task.
    *
    * @param inputFile The input CSV file.
    * @param outputDir The output directory where result CSV file stored.
    */
  def start(inputFile: String, outputDir: String) {
    val conf = new SparkConf().setAppName("WindowProductSessions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder
      .config(conf = conf)
      .getOrCreate()

    try {
      val df = session.read
        .option("header", "true")
        .csv(inputFile)

      // Previous product for the same user
      val userWindow = Window.partitionBy("userId").orderBy("eventTime")
      val lastProduct = lag("product", 1).over(userWindow)
      val lastProductDF = df.withColumn("lastProduct", lastProduct)

      // Mark events by new session flag (product and last product aren't equal)
      val newSession = when(col("lastProduct").isNull, "1")
        .when(col("product")=!=col("lastProduct"), "1")
        .otherwise("0")
      val newSessionDF = lastProductDF.withColumn("newSession", newSession)

      // Global session id by user particular product
      val sessionWindow = Window.orderBy("userId", "eventTime")
      val sessionId = sum("newSession").over(sessionWindow)
      val sessianIdDF = newSessionDF.withColumn("sessionId", sessionId.cast("Int"))

      // Compute start / end session time for each session event
      val sessionPartition = Window.partitionBy("sessionId")
      val sessionStartTime = first("eventTime")
        .over(sessionPartition.orderBy("eventTime"))
      val sessionEndTime = first("eventTime")
        .over(sessionPartition.orderBy(col("eventTime").desc))
      val sessionDF = sessianIdDF
        .withColumn("sessionStartTime", sessionStartTime)
        .withColumn("sessionEndTime", sessionEndTime)

      // Select output columns by event time order
      val outputDF = sessionDF.select("eventTime", "eventType",
        "category", "userId", "product",
        "sessionId", "sessionStartTime", "sessionEndTime")
        .orderBy("eventTime")

      outputDF.write.format("csv")
        .option("header", "true")
        .save(outputDir+"/windowProductSessions")
    }
    finally {
      session.stop()
      sc.stop()
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WindowProductSessions <input_file> <output_dir>")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputDir = args(1)
    start(inputFile, outputDir)
  }
}
