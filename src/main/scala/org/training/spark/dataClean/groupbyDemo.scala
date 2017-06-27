package org.training.spark.dataClean

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/6/22.
 */
object groupbyDemo {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  /*

  1 a
  1 b
  1 c
  2 e
  2 r

  1 a,b,c
  2 e,t


   */


  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"groupbyDemo").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val df1 = sc.parallelize(Seq((1, "a"),(1, "b"), (1, "c"), (2, "b"), (3, "n"),
      (2, "c"), (3, "f"))).toDF("v1", "v2")
    df1.show
    /*
    +---+---+
| v1| v2|
+---+---+
|  1|  a|
|  1|  b|
|  1|  c|
|  2|  b|
|  3|  n|
|  2|  c|
|  3|  f|
+---+---+
     */
    df1.rdd.map{case Row(v1: Int, v2: String) => (v1, v2)}.reduceByKey(_ + "," + _).collect().foreach(println)
/*
(1,a,b,c)
(2,b,c)
(3,n,f)
 */


  }
}
