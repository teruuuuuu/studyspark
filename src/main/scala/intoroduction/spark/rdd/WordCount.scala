package intoroduction.spark.rdd

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

case class WordCountDF(key: String, count: Int)

class WordCount(sc:SparkContext, sqlContext: SQLContext, filePath: String) {
  import sqlContext.implicits._

  lazy val countRDD = {
    sc.textFile(filePath)
      .flatMap(_.split("[ ,.]"))
      .filter(_.matches("""\p{Alnum}+"""))
      .map((_, 1)).reduceByKey(_ + _)
  }

  lazy val countDF = countRDD.toDF
  countDF.createOrReplaceTempView("wordcount_tbl")
  def getRDD = countRDD
  def getDF = countDF


  def getByKey(key: String) = {
    countDF.where(countDF("_1") === key).head
  }
}

/**
  * 実行コマンド
  * spark-submit --master yarn \
  * --class intoroduction.spark.rdd.WordCount \
  * --name WordCount target/scala-2.11/studyspark.jar \
  * /input/test.txt
  *
  */
object WordCount {

  def main(args: Array[String]) = {
    require(args.length >= 1, "ファイルを指定してください")
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val filePath = args(0)

    try {

      val wordCount = new WordCount(sc, sqlContext, filePath)
      wordCount.getRDD.foreach(println)

    } finally {
      sc.stop()
    }
  }
}
