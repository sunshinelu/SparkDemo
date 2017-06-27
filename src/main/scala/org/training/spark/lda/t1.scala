package org.training.spark.lda

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/24.
 */
object t1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(s"LDATest2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val train = sc.textFile("hdfs://cdh01:8020//user/data/sogou2/JBtrain", 400)
    val test = sc.textFile("hdfs://cdh01:8020//user/data/sogou2/JBtest", 400)
    val same = sc.textFile("hdfs://cdh01:8020//user/data/sogou2/same", 400)
    same.filter { x => !x.contains('=') }.count()
    val sameWord = same.map { line =>
      val valuekey = line.split('=')
      (valuekey(1), valuekey(0))
    }.collect()
    val broadcastVar = sc.broadcast(sameWord)
    val diffTrain = train.map { line =>
      val broad = broadcastVar.value
      val regex = """^\d+$""".r
      val temp = line.split("\t")
      val wordArray = temp(4).split(",")
      var str = ""
      for (word <- wordArray) {
        val keyVal = broad.filter(line => line._1.equals(word))
        if (keyVal.length > 0) {
          val oneKeyVal = keyVal(0)
          str = str + "#" + oneKeyVal._2
        } else if (regex.findFirstMatchIn(word) == None) {
          str = str + "#" + word
        }
      }
      (temp(0), temp(1), temp(2), temp(3), str)
    }
    diffTrain.toDF().coalesce(1).write.csv("hdfs://cdh01:8020//user/data/sogou2/ReplaceJBtrain")

    val diffTest = test.map { line =>
      val broad = broadcastVar.value
      val regex = """^\d+$""".r
      val temp = line.split("\t")
      val wordArray = temp(1).split(",")
      var str = ""
      for (word <- wordArray) {
        val keyVal = broad.filter(line => line._1.equals(word))
        if (keyVal.length > 0) {
          val oneKeyVal = keyVal(0)
          str = str + "#" + oneKeyVal._2
        } else if (regex.findFirstMatchIn(word) == None) {
          str = str + "#" + word
        }
      }
      (temp(0), str)
    }
    diffTest.toDF().coalesce(1).write.csv("hdfs://cdh01:8020//user/data/sogou2/ReplaceJBtest")



    val diffAll = diffTrain.map(_._5).union(diffTest.map(_._2)).flatMap(_.split("#")).map((_, 1)).reduceByKey(_ + _).collect.filter(line => line._1 != "" && line._2 > 14).map(line => line._1)

    sc.stop()

  }
}
