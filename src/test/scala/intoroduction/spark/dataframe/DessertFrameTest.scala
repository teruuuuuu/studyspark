package intoroduction.spark.dataframe

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class DessertFrameTest extends FunSuite with BeforeAndAfterAll{
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  override def beforeAll() {
    print("before...")
    sparkConf = new SparkConf().setAppName("WordCountTest").setMaster("local")
    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    sc.stop()
    print("after...")
  }

  test("dessert_frame"){
    val filePath = "src/test/resources/data/dessert-menu.csv"
    val desertFrame = new DesertFrame(sc, sqlContext, filePath)

    val d19DF = desertFrame.findByMenuId("D-19").head
    assert(d19DF.get(0) == "D-19")
    assert(d19DF.get(1) == "キャラメルロール")
    assert(d19DF.get(2) == 370)
    assert(d19DF.get(3) == 230)
  }

}
