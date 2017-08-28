package intoroduction.spark.rdd

import java.io.{BufferedReader, InputStreamReader, Reader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{HashMap, Map}

object BestSellerFinder {

  /**
    * 商品ID, 販売個数を要素にもつRDDを生成
    * @param csvFile
    * @param sc
    */
  private def createSalesRDD(csvFile: String, sc: SparkContext): RDD[(String, Int)] = {
    val logRDD = sc.textFile(csvFile)
    logRDD.map{ record =>
      val splitRecord = record.split(",")
      val productId = splitRecord(2)
      val numOfSold = splitRecord(3).toInt
      (productId, numOfSold)
    }
  }

  private def createOver50SoldRDD(rdd: RDD[(String, Int)]): RDD[(String, Int)] = {
    rdd.reduceByKey(_ + _).filter(_._2 >= 50)
  }

  private def loadCSVIntoMap(productsCSVFile: String): Map[String, (String, Int)] = {
    var productsCSVReader: Reader = null

    try {
      val productsMap = new HashMap[String, (String, Int)]
      val hadoopConf = new Configuration
      val fileSystem = FileSystem.get(hadoopConf)
      val inputStream = fileSystem.open(new Path(productsCSVFile))
      val productsCSVReader = new BufferedReader(new InputStreamReader(inputStream))
      var line = productsCSVReader.readLine

      while (line != null){
        val splitLine = line.split(",")
        val productId = splitLine(0)
        val productName = splitLine(1)
        val unitPrice = splitLine(2).toInt

        productsMap(productId) = (productName, unitPrice)
        line = productsCSVReader.readLine
      }
      productsMap
    } finally {
      if(productsCSVReader != null){
        productsCSVReader.close()
      }
    }
  }

  private def createResultRDD(broadcastedMap: Broadcast[_ <: Map[String, (String, Int)]], rdd:RDD[(String, Int)]): RDD[(String, Int, Int)] = {
    rdd.map {
      case (productId, amount) =>
        val productsMap = broadcastedMap.value
        val (productName, unitPrice) = productsMap(productId)
        (productName, amount, amount * unitPrice)
    }
  }


  /**
    * spark-submit --master yarn \
    * --class intoroduction.spark.rdd.BestSellerFinder \
    * --name BestSellerFinder target/scala-2.11/studyspark.jar
    * @param args
    */
  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("BestSellerFinder").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val salesCSVFile1 = "data/bestSellerFinder_sales1.csv"
      val salesCSVFile2 = "data/bestSellerFinder_sales2.csv"
      val productsCSVFile = "data/bestSellerFinder_products.csv"
      val outputPath = "data/bestSellerFinder_result"

      val salesRDD1 = createSalesRDD(salesCSVFile1, sc)
      val salesRDD2 = createSalesRDD(salesCSVFile2, sc)

      val over50SoldRDD1 = createOver50SoldRDD(salesRDD1)
      val over50SoldRDD2 = createOver50SoldRDD(salesRDD2)

      //val botherOver50SoldRDD = over50SoldRDD1.join(over50SoldRDD2)
      val botherOver50SoldRDD = over50SoldRDD1.leftOuterJoin(over50SoldRDD2)
      val over50SoldAndAmountRDD = botherOver50SoldRDD.map {
        case (productId, (amount1, None)) =>
          (productId, amount1)
        case (productId, (amount1, Some(amount2))) =>
          (productId, amount1 + amount2)
      }

      val productsMap = loadCSVIntoMap(productsCSVFile)
      val broadcastedMap = sc.broadcast(productsMap)

      val resultRDD = createResultRDD(broadcastedMap, over50SoldAndAmountRDD)
      resultRDD.foreach(println)
      resultRDD.saveAsTextFile(outputPath)
    } finally {
      sc.stop()
    }

  }
}
