package org.training.spark.sentiment

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.Pipeline

/**
 * Created by sunlu on 17/6/5.
 * https://databricks.com/wp-content/uploads/2015/10/STEP-3-Sentiment_Analysis.html
 *
 */


object sentimentDemo1 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

/*
  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentDemo1").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val data = table("tweetData") unionAll table("reviewData")

    val trainingData = data.withColumn("label", when(data("isHappy"), 1.0D).otherwise(0.0D))

    val tokenizer = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\p{L}+")
      .setInputCol("text")
      .setOutputCol("words")

    val stopwords: Array[String] = sc.textFile("/mnt/hossein/text/stopwords.txt").flatMap(_.stripMargin.split("\\s+")).collect ++ Array("rt")

    val filterer = new StopWordsRemover()
      .setStopWords(stopwords)
      .setCaseSensitive(false)
      .setInputCol("words")
      .setOutputCol("filtered")

    val countVectorizer = new CountVectorizer()
      .setInputCol("filtered")
      .setOutputCol("features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.2)
      .setElasticNetParam(0.0)

    def toEmotion(pred: Double): String = if (pred == 1.0) "positive" else "negative"
    spark.udf.register("toEmotion", toEmotion _)

    val testText = getArgument("test", "I hate that")
    case class Tweet(text: String, label: Double)
    val testDF = sc.makeRDD(Seq(Tweet(testText, -1.0D))).toDF
    display(lrModel.transform(testDF).selectExpr("text", "toEmotion(prediction) as sentiment"))

    val pipeline = new Pipeline().setStages(Array(tokenizer, filterer, countVectorizer, lr))

  }
  */
}
