package org.training.spark.lda

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/4/21.
 */
class test {
  val h = "hello word"
  def toString2(s: String): Unit ={
    val s1 = s.toString
    print(s1)
  }

}
object test extends App{
  val s = new test()
  println("=====")
  println(s.toString2("hello Word==="))
  println(s.toString())
  println(s.h)

  val conf = new SparkConf().setAppName(s"Test").setMaster("local[*]").set("spark.executor.memory", "2g")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext
  val textFile = sc.textFile("data/textfile/Hamlet.txt")
  val text1 = textFile.flatMap(line => line.split(" ")).map((_, 1)).reduceByKey(_ + _).collect.
    filter(line => line._1 != "" && line._2 > 14).map(line => line._1)
  val cvm = new CountVectorizerModel(text1)//.setInputCol(tokenizer.getOutputCol).setOutputCol("features")



}