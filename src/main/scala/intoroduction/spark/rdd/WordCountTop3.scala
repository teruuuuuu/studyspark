package intoroduction.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  */
object WordCountTop3 {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val filePath = "data/wordcount.md"
      val wordAndCountRDD = sc.textFile(filePath)
                .flatMap(_.split("[ ,.]"))
                .filter(_.matches("""\p{Alnum}+"""))
                .map((_, 1)).reduceByKey(_ + _)

      val top3Words = wordAndCountRDD.map {
        case( word, count) => (count, word)
      }.sortByKey(false).map {
        case (count, word) => (word, count)
      }.take(3)

      top3Words.foreach(println)

    } finally {
      sc.stop()
    }
  }
}
