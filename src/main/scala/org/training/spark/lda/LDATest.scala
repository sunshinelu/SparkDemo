package org.training.spark.lda

import edu.stanford.nlp.process.Morphology
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizerModel, CountVectorizer, StopWordsRemover, RegexTokenizer}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, OnlineLDAOptimizer, EMLDAOptimizer, LDA}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by sunlu on 17/4/20.
 */
object LDATest {

  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }

  def main(args: Array[String]) {

    SetLogger

    val conf = new SparkConf().setAppName(s"LDATest").setMaster("local[*]").set("spark.executor.memory", "2g")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val paths = "data/docs/"
    val stopwordFile = "data/stopWords.txt"
    //Reading the Whole Text Files
    val initialrdd = spark.sparkContext.wholeTextFiles(paths).map(_._2)
  //  initialrdd.take(5).foreach(println)
    initialrdd.cache()
    //initialrdd.collect().foreach(print)

    val rdd = initialrdd.mapPartitions { partition =>
      val morphology = new Morphology()
      partition.map { value =>
        LDAHelper.getLemmaText(value, morphology)
      }
    }.map(LDAHelper.filterSpecialCharacters)


    rdd.cache()
    initialrdd.unpersist()
    val df = rdd.toDF("docs")
    val customizedStopWords: Array[String] = if (stopwordFile.isEmpty) {
      Array.empty[String]
    } else {
      val stopWordText = sc.textFile(stopwordFile).collect()
      stopWordText.flatMap(_.stripMargin.split(","))
    }
    //Tokenizing using the RegexTokenizer(分词)
    val tokenizer = new RegexTokenizer().setInputCol("docs").setOutputCol("rawTokens")

    //Removing the Stop-words using the Stop Words remover
    val stopWordsRemover = new StopWordsRemover().setInputCol("rawTokens").setOutputCol("tokens")
    stopWordsRemover.setStopWords(stopWordsRemover.getStopWords ++ customizedStopWords)

    //Converting the Tokens into the CountVector
    val vocabSize: Int = 2900000
    val countVectorizer = new CountVectorizer().setVocabSize(vocabSize).setInputCol("tokens").setOutputCol("features")

    //Setting up the pipeline
    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)
    val documents = model.transform(df).select("features").rdd.map {
      case Row(features: MLVector) => Vectors.fromML(features)
    }.zipWithIndex().map(_.swap)


    val corpus = documents
     val vocabArray = model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary // vocabulary
     val actualNumTokens = documents.map(_._2.numActives).sum().toLong // total token count



    val actualCorpusSize = corpus.count()
    val actualVocabSize = vocabArray.length

    corpus.cache()
    println()
    println(s"Corpus summary:")
    println("documents: " + corpus.collect.foreach(println))

    println(s"\t Training set size: $actualCorpusSize documents")

    println("vocabArray: " + vocabArray.mkString(";"))
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $actualNumTokens tokens")
    println("actualNumTokens: " + actualNumTokens)
    println()

    // Run LDA.
    val lda = new LDA()

    val algorithm: String = "em"

    val optimizer = algorithm.toLowerCase match {
      case "em" => new EMLDAOptimizer
      // add (1.0 / actualCorpusSize) to MiniBatchFraction be more robust on tiny datasets.
      case "online" => new OnlineLDAOptimizer().setMiniBatchFraction(0.05 + 1.0 / actualCorpusSize)
      case _ => throw new IllegalArgumentException(
        s"Only em, online are supported but got ${algorithm}.")
    }

    val k: Int = 5
    val maxIterations: Int = 10
    val docConcentration: Double = -1
    val topicConcentration: Double = -1
    val checkpointDir: Option[String] = None
    val checkpointInterval: Int = 10

    lda.setOptimizer(optimizer)
      .setK(k)
      .setMaxIterations(maxIterations)
      .setDocConcentration(docConcentration)
      .setTopicConcentration(topicConcentration)
      .setCheckpointInterval(checkpointInterval)
    if (checkpointDir.nonEmpty) {
      sc.setCheckpointDir(checkpointDir.get)
    }
    val startTime = System.nanoTime()
    val ldaModel = lda.run(corpus)
    val elapsed = (System.nanoTime() - startTime) / 1e9

    println(s"Finished training LDA model.  Summary:")
    println(s"\t Training time: $elapsed sec")

    if (ldaModel.isInstanceOf[DistributedLDAModel]) {
      val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distLDAModel.logLikelihood / actualCorpusSize.toDouble
      println(s"\t Training data average log likelihood: $avgLogLikelihood")
      println()
    }


    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val topics = topicIndices.map { case (terms, termWeights) =>
      terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
    }
    println(s"${k} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
      }
      println()
    }

    sc.stop()
  }
}
