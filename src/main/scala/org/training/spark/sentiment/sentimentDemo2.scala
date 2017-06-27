package org.training.spark.sentiment

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
//import org.apache.spark.streaming.twitter._

/**
 * Created by sunlu on 17/6/5.
 *
 * http://www.tuicool.com/articles/6FjmIrq
 *
 */
object sentimentDemo2 {
  /*
  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("twitter-stream-sentiment")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(5))

    System.setProperty("twitter4j.oauth.consumerKey", "consumerKey")
    System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecret")
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", "accessTokenSecret")

    val stream = TwitterUtils.createStream(ssc, None)

    val tags = stream.flatMap { status =>
      status.getHashtagEntities.map(_.getText)
    }
    tags.countByValue()
      .foreachRDD { rdd =>
        val now = org.joda.time.DateTime.now()
        rdd
          .sortBy(_._2)
          .map(x => (x, now))
          .saveAsTextFile(s"~/twitter/$now")
      }

    val tweets = stream.filter {t =>
      val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      tags.contains("#bigdata") && tags.contains("#food")
    }



  }
  */
}
