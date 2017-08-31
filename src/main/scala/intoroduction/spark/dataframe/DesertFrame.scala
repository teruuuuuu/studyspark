package intoroduction.spark.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DataType._
import org.apache.spark.sql.types.IntegerType

case class Dessert(menuId: String, name: String, price: Int, kcal: Int)


class DesertFrame(sc: SparkContext, sqlContext: SQLContext, filePath: String) {
  import sqlContext.implicits._
  lazy val dessertDF = {
    val dessertRDD = sc.textFile(filePath)
    sc.textFile(filePath)
    // データフレームとして読み込む
    dessertRDD.map { record =>
      val splitRecord = record.split(",")
      val menuId = splitRecord(0)
      val name = splitRecord(1)
      val price = splitRecord(2).toInt
      val kcal = splitRecord(3).toInt
      Dessert(menuId, name, price, kcal)
    }.toDF
  }
  dessertDF.createOrReplaceTempView("desert_table")


  def findByMenuId(menuId: String) = {
    dessertDF.where(dessertDF("menuId") === menuId)
  }

  def findByHighCalorie(kcal: Int) = {
    dessertDF.where(dessertDF("kcal") >= kcal).orderBy($"price".asc, $"kcal".desc)
  }

  def findByHighCorieCount(kcal: Int) = {
    sqlContext.sql("SELECT count(*) AS num_of_over300Kcal FROM desert_table WHERE kcal >= " + kcal)
  }
}

/**
  * 実行コマンド
  * spark-submit --master yarn \
  * --class intoroduction.spark.dataframe.DesertFrame \
  * --name DesertFrame \
  * target/scala-2.11/studyspark.jar
  */
object DesertFrame {

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("DesertFrame").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dessertRDD = sc.textFile("src/main/hive/dessert-menu.csv")
    // データフレームとして読み込む
    val dessertDF = dessertRDD.map { record =>
      val splitRecord = record.split(",")
      val menuId = splitRecord(0)
      val name = splitRecord(1)
      val price= splitRecord(2).toInt
      val kcal = splitRecord(3).toInt
      Dessert(menuId, name, price, kcal)
    }.toDF

    dessertDF.printSchema

    // データフレームからRDDを生成する
    val rowRDD = dessertDF.rdd
    val nameAndPriceRDD = rowRDD.map { row =>
      val name = row.getString(1)
      val price = row.getInt(2)
      (name, price)
    }
    nameAndPriceRDD.collect.foreach(println)

    // クエリを発行する
    dessertDF.createOrReplaceTempView("desert_table")
    val numOver300KcalDF = sqlContext.sql(
      """
        |SELECT count(*) AS num_of_over300Kcal FROM desert_table WHERE kcal >= 260
      """.stripMargin)
    numOver300KcalDF.show

    // Sqpark SQLの組み込み関数を利用してみる
    sqlContext.sql("SELECT atan2(1, 3) AS `ATAN2(1, 3)`").show
    sqlContext.sql("SELECT pi() AS PI, e() AS E").show

    // 表示する列の指定
    val nameAndPriceDF = dessertDF.select(dessertDF("name"), dessertDF("price"))
    nameAndPriceDF.show

    val selectAllDF = dessertDF.select("*")
    selectAllDF.printSchema

    nameAndPriceDF.show

    // 件数指定
    selectAllDF.show(3)


    // 演算する
    val nameAndDollarDF = nameAndPriceDF.select($"name", $"price" / 120.0)
    nameAndDollarDF.printSchema
    nameAndDollarDF.show

    // where
    val over520YenDF = dessertDF.where($"price" >= 520)
    over520YenDF.printSchema
    over520YenDF.show

    // select
    val over520YenNameDF = over520YenDF.select($"name")
    over520YenNameDF.printSchema
    over520YenNameDF.show


    // そーと
    val sortedDessertDF = dessertDF.orderBy($"price".asc, $"kcal".desc)
    sortedDessertDF.printSchema
    sortedDessertDF.show

    // 集約
    //val avgKcalDF = dessertDF.agg(avg($"kcal") as "avg_ok_kcal")
    //val numPerPriceRangeDF = dessertDF.groupBy((($"price" / 100) cast IntegerType) * 100 as "price_rage").agg(count($"price"))



  }

}
