package org.training.spark.lda

import java.util.Properties

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.SparkSession
import org.training.spark.lda.LDATest2.ArticalLable

/**
 * Created by sunlu on 17/4/24.
 * 使用TF-IDF进行矩阵转化的方法构建LDA模型
 *
 * 运行失败！
 */
object LDATest3 {
  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {
    SetLogger

    val conf = new SparkConf().setAppName(s"LDATest2").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //load stopwords file
    val stopwordsFile = "data/Stopwords.dic"
    val stopwords = sc.textFile(stopwordsFile).collect().toList

    //connect mysql database
    val url1 = "jdbc:mysql://localhost:3306/sunluMySQL"
    val prop1 = new Properties()
    prop1.setProperty("user", "root")
    prop1.setProperty("password", "root")
    //get data
    val ds1 = spark.read.jdbc(url1, "ylzx_Example", prop1)
    val rdd1 = ds1.rdd.map(r => (r(0), r(1), r(2))).map{x =>
      val rowkey = x._1.toString
      val title = x._2.toString
      val content = x._3.toString
      val segWords = ToAnalysis.parse(content).toArray.map(_.toString.split("/")).
        filter(_.length >= 2).map(_ (0)).toList.
        filter(word => word.length >= 2 & !stopwords.contains(word)).toSeq
      (rowkey, segWords)
    }.zipWithIndex().map{x =>
      val rowkey = x._1._1.toString
      val segWords = x._1._2
      val id = x._2
      ArticalLable(id, segWords, rowkey)
    }
    val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
    //这里将每一行的行号作为doc id，每一行的分词结果生成tf词频向量

    val tf_num_pairs = rdd1.map { x => {
      val tf = hashingTF.transform(x.segWords)
      (x.id, tf)
    }
    }
    tf_num_pairs.persist()

    //构建idf model
    val idf = new IDF().fit(tf_num_pairs.values)

    //将tf向量转换成tf-idf向量
    val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))

    val tfidf = num_idf_pairs.map { case (i, v) => new IndexedRow(i, v) }

    val indexed_matrix = new IndexedRowMatrix(tfidf)

    val transposed_matrix = indexed_matrix.toCoordinateMatrix.transpose()

    val corpus = transposed_matrix.toIndexedRowMatrix().rows.map{r =>
      (r.index, r.vector)

    }

    val ldaModel = new LDA().setK(5).run(corpus).asInstanceOf[DistributedLDAModel]

    /*
    //输出训练主题
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)){
      print("Topic " + topic + ":")
      for(word <- Range(0, ldaModel.vocabSize)){
        print("%.5f".format(topics(word, topic)))
      }
      println()
    }
*/

    val topics = ldaModel.describeTopics(5)
    for (k <- Range(0, 3)){
      print("Topic " + k + ": ")
      for (w <- Range(0, 5)){
        print("%d=%.5f".format(topics(k)._1(w), topics(k)._2(w)))
      }
      println()
    }

    sc.stop()
  }
}
