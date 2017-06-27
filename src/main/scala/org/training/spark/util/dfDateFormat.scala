package org.training.spark.util


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Created by sunlu on 17/6/9.
 */
object dfDateFormat {


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"timeSeries").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df = Seq((1L, "01-APR-2015"),(2L, "01-APR-2016"),(3L, "01-APR-2017")).toDF("id", "ts")

    val df2 = df.select(to_date(unix_timestamp(
      $"ts", "dd-MMM-yyyy"
    ).cast("timestamp")).alias("timestamp"))
    println("show df2: ")
    df2.show()
    df2.printSchema()

    val df3 = df.withColumn("toDate",unix_timestamp(
      $"ts", "dd-MMM-yyyy"
    ).cast("int").alias("timestamp")).withColumn("current", current_date()).withColumn("timestamp", current_timestamp())
    println("show df3: ")
    df3.show

//    df3.withColumn("minDate", min($"timestamp")).show

    val minMaxDf = df2.agg(min("timestamp"),max("timestamp"))
    minMaxDf.printSchema()
    val minusDate = minMaxDf.withColumn("minus", datediff(col("max(timestamp)"),col("min(timestamp)")))
    println("show minusDate: ")
    minusDate.show()

   val minDate = minMaxDf.first().get(0)
    val maxDate = minMaxDf.first().get(1)
    println("minDate is: " + minDate)
    println("maxDate is: " + maxDate)



   // val df3 = df.select(to_date($"ts", "dd-MMM-yyyy").alias("date"))

  }
}
