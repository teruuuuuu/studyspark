package intoroduction.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 実行コマンド
  * spark-submit --master yarn \
  * --class intoroduction.spark.rdd.WordCount \
  * --name WordCount target/scala-2.11/studyspark.jar
  *
  */
object WordCount {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val filePath = "data/wordcount.md"
      val wordAndCountRDD = sc.textFile(filePath)
                .flatMap(_.split("[ ,.]"))
                .filter(_.matches("""\p{Alnum}+"""))
                .map((_, 1)).reduceByKey(_ + _)

      wordAndCountRDD.collect.foreach(println)

    } finally {
      sc.stop()
    }
  }
}
