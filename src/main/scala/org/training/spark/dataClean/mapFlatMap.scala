package org.training.spark.dataClean

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by sunlu on 17/6/22.
 */
object mapFlatMap {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(s"mapFlatMap").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val rdd1 = sc.parallelize(Seq((1, "one two three four five six seven"),
      (2, "one two three four five six seven"), (3, "one two three four five six seven")))
    val rdd2 = rdd1.flatMap {
      case (id, manuallabel) => manuallabel.split(" ").map((id, _))
    }
    rdd2.collect().foreach(println)
    /*
    (1,one)
(1,two)
(1,three)
(1,four)
(1,five)
(1,six)
(1,seven)
(2,one)
(2,two)
(2,three)
(2,four)
(2,five)
(2,six)
(2,seven)
(3,one)
(3,two)
(3,three)
(3,four)
(3,five)
(3,six)
(3,seven)
     */

    val rdd3 = rdd2.map(x => {
      val x2 = x._2.split(" ")
      (x._1, x2.toIterable)
    }).flatMap { x =>
      val y = x._2
      for (w <- y) yield (x._1, w)
    }
    rdd3.collect.foreach(println)
    /*
    (1,one)
(1,two)
(1,three)
(1,four)
(1,five)
(1,six)
(1,seven)
(2,one)
(2,two)
(2,three)
(2,four)
(2,five)
(2,six)
(2,seven)
(3,one)
(3,two)
(3,three)
(3,four)
(3,five)
(3,six)
(3,seven)
     */
  }
}
