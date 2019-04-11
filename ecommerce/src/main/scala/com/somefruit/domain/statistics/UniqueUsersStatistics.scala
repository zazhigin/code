package com.somefruit.domain.statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp, when}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zazhigin on 10.04.19.
  */
object UniqueUsersStatistics {

  /**
    * Starts unique users statistics task.
    *
    * @param inputFile The input CSV file.
    * @param outputDir The output directory where result CSV file stored.
    * @param minDuration The minimum duration in minutes format.
    * @param maxDuration The maximum duration in minutes format.
    */
  def start(inputFile: String, outputDir: String, minDuration: Int, maxDuration: Int): Unit = {
    val conf = new SparkConf().setAppName("UniqueUsersStatistics").setMaster("local[*]")
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
      val sessionDF = session.sql("SELECT category, userId, sessionId, sessionStartTime, sessionEndTime"+
        " FROM sessions GROUP BY category, userId, sessionId, sessionStartTime, sessionEndTime")

      // Calculate session duration and flags by three groups (less min, min to max & greater max)
      val duration =
        unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime"))
      val lessMin = when(
        col("duration") < minDuration * 60,"1").otherwise(0)
      val minMax = when(
        col("duration") >= minDuration * 60 && col("duration") < maxDuration * 60,"1")
        .otherwise(0)
      val grtrMax = when(
        col("duration") >= maxDuration * 60,"1").otherwise(0)
      val uniqueDF = sessionDF
        .withColumn("duration", duration)
        .withColumn("lessMin", lessMin)
        .withColumn("minMax", minMax)
        .withColumn("grtrMax", grtrMax)

      // Find users by each of three groups
      uniqueDF.createOrReplaceTempView("sessions")
      val lessMinDF = session.sql(
        "SELECT category, userId FROM sessions WHERE lessMin == 1"+
          " GROUP BY category, userId ORDER BY category, userId")
      val minMaxDF = session.sql(
        "SELECT category, userId FROM sessions WHERE minMax == 1"+
          " GROUP BY category, userId ORDER BY category, userId")
      val grtrMaxDF = session.sql(
        "SELECT category, userId FROM sessions WHERE grtrMax == 1"+
          " GROUP BY category, userId ORDER BY category, userId")

      lessMinDF.coalesce(1)
        .write.format("csv").option("header", "true").save(outputDir+"/uniqueUsersStatistics/lessMin")
      minMaxDF.coalesce(1)
        .write.format("csv").option("header", "true").save(outputDir+"/uniqueUsersStatistics/minMax")
      grtrMaxDF.coalesce(1)
        .write.format("csv").option("header", "true").save(outputDir+"/uniqueUsersStatistics/grtrMax")
    }
    finally {
      session.stop()
      sc.stop()
    }
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: UniqueUsersStatistics <input_file> <output_dir> <min_duration> <max_duration>")
      System.exit(1)
    }
    val inputFile = args(0)
    val outputDir = args(1)
    val minDuration = args(2).toInt
    val maxDuration = args(3).toInt
    start(inputFile, outputDir, minDuration, maxDuration)
  }
}
