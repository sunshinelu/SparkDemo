package org.training.spark.sentiment

import java.util.Properties

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.training.spark.sentiment.sentimentWordCount.Corpus
import org.training.spark.sql.truncateMysql
import org.training.spark.util.timeSeries

/**
 * Created by sunlu on 17/6/9.
 * 本地运行成功！
 * 修改sentimentWordCount中的部分函数使用，
 * 比如将String类型的时间串转成时间类型，获取当前时间使用current_timestamp，调整时间格式使用date_format，而不是使用自己写的函数。
 */
object sentimentWordCountV2 {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF)
  }

  case class Book(title: String, words: String)

  case class Corpus(id: String, content: String, segWords: String, time: String, tokens: String)

  case class mysqlSchema(date: String, lable: String, value: Double, sysTime: String)

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"sentimentWordCount").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //获取正类、负类词典。posnegDF在join时使用；posnegList在词过滤时使用。
    val posnegDF = spark.read.format("CSV").option("header", "true").load("data/sentiment/posneg.csv")

    val posnegList = posnegDF.select("term").dropDuplicates().rdd.map { case Row(term: String) => term }.collect().toList

    //load stopwords file
    val stopwordsFile = "data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/bbs"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "article", prop1)

    //定义UDF
    //分词、停用词过滤、正类、负类词过滤
    val segWorsd = udf((content: String) => {
      ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 1 & !stopwords.contains(word)).filter(word => posnegList.contains(word))
        .toSeq.mkString(" ")
    })
    //时间处理
    val replaceString = udf((content: String) => {
      content.replace("时间：", "") //.split(" ")(0)
    })
    val ds2 = ds1.na.drop(Array("content")).select("id", "content", "time").withColumn("segWords", segWorsd(column("content"))).
      withColumn("time2", replaceString(col("time"))).drop("time")
    val ds3 = ds2.as[(String, String, String, String)]
    val ds4 = ds3.flatMap {
      case (x1, x2, x3, x4) => x3.split(" ").map(Corpus(x1, x2, x3, x4, _))
    }.toDF

    // 对time列的String类型改为时间类型
    val ds5 = ds4.withColumn("time", to_date(unix_timestamp(
      $"time", "yyyy-MM-dd HH:mm:ss"
    ).cast("timestamp")))

    val ds6 = ds5.join(posnegDF, ds5("tokens") === posnegDF("term"), "left").na.drop()

    val ds7 = ds6.groupBy("id", "time").agg(sum("weight")).withColumnRenamed("sum(weight)", "value")

    val sentimentLabel = udf((value: Int) => {
      value match {
        case x if x > 0 => "正类"
        case x if x < 0 => "负类"
        case _ => "中性"
      }
    })

    val ds8 = ds7.withColumn("label", sentimentLabel(col("value")))

    val ds9 = ds8.groupBy("time", "label").agg(count("label")).withColumnRenamed("count(label)", "value")
    val columnsRenamed = Seq("time", "label", "value")
    val ds10 = ds9.toDF(columnsRenamed: _*)

    val labelUdf = udf((arg: String) => {
      "正类,负类,中性"
    })
    //获取时间差
    val n = ds10.select(datediff(max($"time"), min($"time"))).first().get(0).toString.toInt
    //println("n value is: " + n)
    val tsDF = sc.parallelize(timeSeries.gTimeSeries(n)).toDF("time").
      withColumn("labels", labelUdf($"time")).as[(String, String)]
    val tsDF_2ColumnsName = Seq("time", "label")
    val tsDF_2 = tsDF.flatMap {
      case (timeSeries, labels) => labels.split(",").map((timeSeries, _))
    }.toDF(tsDF_2ColumnsName: _*)

    //对指定的列空值填充，改变value列的数据类型，获取当前时间，对当前时间进行格式化
    val ds11 = tsDF_2.join(ds10, Seq("time", "label"), "left").na.fill(value = 0, cols = Array("value")).
      withColumn("value", $"value".cast("int")).withColumn("sysTime", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

    //将ds11保存到mysql表中
    val url2 = "jdbc:mysql://localhost:3306/bbs?useUnicode=true&characterEncoding=UTF-8"
    //使用"?useUnicode=true&characterEncoding=UTF-8"以防止出现存入MySQL数据库中中文乱码情况
    val prop2 = new Properties()
    prop2.setProperty("user", "root")
    prop2.setProperty("password", "root")

    //清空sentiment_wc表
    truncateMysql.truncateMysql2("jdbc:mysql://localhost:3306/bbs", "root", "root", "sentiment_wc")

    //将结果保存到数据框中
    ds11.write.mode("append").jdbc(url2, "sentiment_wc", prop2) //overwrite or append

    println("run succeeded!")

  }
}
