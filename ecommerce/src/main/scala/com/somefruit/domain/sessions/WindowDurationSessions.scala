package com.somefruit.domain.sessions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zazhigin on 10.04.19.
  */
object WindowDurationSessions {

  /**
    * Starts window duration sessions task.
    *
    * @param inputFile The input CSV file.
    * @param outputDir The output directory where result CSV file stored.
    * @param sessionDuration The session duration in minutes format.
    */
  def start(inputFile: String, outputDir: String, sessionDuration: Int) {
    val conf = new SparkConf().setAppName("WindowDurationSessions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val session = SparkSession.builder
      .config(conf = conf)
      .getOrCreate()

    try {
      val df = session.read
        .option("header", "true")
        .csv(inputFile)

      // Previous event time for the same user
      val userWindow = Window.partitionBy("userId").orderBy("eventTime")
      val lastEvent = lag("eventTime", 1).over(userWindow)
      val lastEventDF = df.withColumn("lastEvent", lastEvent)

      // Period of inactivity same user since last event
      val inactivity = when(col("lastEvent").isNotNull,
        unix_timestamp(col("eventTime")) - unix_timestamp(col("lastEvent")))
        .otherwise(null)
      val inactivityDF = lastEventDF.withColumn("inactivity", inactivity)

      // Mark events by new session flag (period of inactivity more than specified minutes)
      val newSession = when(col("inactivity").isNull, "1")
        .when(col("inactivity") >= sessionDuration * 60, "1")
        .otherwise("0")
      val newSessionDF = inactivityDF.withColumn("newSession", newSession)

      // Global session id by either user distinction / user inactivity time
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
        .save(outputDir+"/windowDurationSessions")
    }
    finally {
      session.stop()
      sc.stop()
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: WindowDurationSessions <input_file> <output_dir> <session_duration>")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputDir = args(1)
    val sessionDuration = args(2).toInt
    start(inputFile, outputDir, sessionDuration)
  }
}
