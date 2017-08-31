package intoroduction.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class WordCountTest extends FunSuite with BeforeAndAfterAll{
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  override def beforeAll() {
    print("before...")
    sparkConf = new SparkConf().setAppName("WordCountTest").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  override def afterAll() {
    sc.stop()
    print("after...")
  }

  test("word_count"){
    val sqlContext = new SQLContext(sc)

    val filePath = "src/test/resources/data/wordCount.txt"
    val wordCount = new WordCount(sc, sqlContext, filePath)
    wordCount.getRDD.foreach(println)

    wordCount.getDF.printSchema
    wordCount.getDF.show

    // key指定で値が正しいかのチェック
    assert(wordCount.getByKey("a").get(1) === 2)
  }
}
