package intoroduction.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by arimuraterutoshi on 2017/08/25.
  * spark-submit --master yarn \
  * --class intoroduction.spark.rdd.QuestionnaireSummarization \
  * --name QuestionnaireSummarization target/scala-2.11/studyspark.jar \
  * data/questionnaireSummarization.csv
  */
object QuestionnaireSummarization {

  /**
    * アンケートの評価の平均値取得
    * @param rdd
    * @return
    */
  private def computeAllAvg(rdd: RDD[(Int, String, Int)]) = {
    val (totalPoint, count: Int) =
      rdd.map(record => (record._3, 1)).reduce {
        case ((intermedPoint, intermedCount), (point, one)) =>
          (intermedPoint + point, intermedCount + one)
      }
    totalPoint / count.toDouble
  }

  private def computeAgeRangeAvg(rdd: RDD[(Int, String, Int)]) = {
    rdd.map(record => (record._1, (record._3, 1))).reduceByKey {
      case ((intermedPoint , intermedCount), (point, count)) =>
        (intermedPoint + point, intermedCount + count)
    }.map {
      case (ageRange, (totalPoint, count)) =>
        (ageRange, totalPoint/ count.toDouble)
    }.collect
  }

  private def computeMorFAvg(rdd: RDD[(Int, String, Int)],
                             numMAcc: Accumulator[Int], totalPointMAcc: Accumulator[Int],
                             numFAcc: Accumulator[Int],  totalPointFAcc: Accumulator[Int]) = {
    rdd.foreach {
      case (_, maleOrFemale, point) =>
        maleOrFemale match {
          case "M" =>
            numMAcc += 1
            totalPointMAcc += point
          case "F" =>
            numFAcc += 1
            totalPointFAcc += point
        }
    }
    Seq(("Male", totalPointMAcc.value / numMAcc.value.toDouble),
      ("Female", totalPointFAcc.value / numMAcc.value.toDouble))
  }


  def main(args: Array[String]): Unit ={

    require(args.length >= 1, "ファイルを指定してください")

    val conf = new SparkConf().setAppName("QuestionnaireSummarization").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try{

      val filePath = args(0)
      val questionnaireRDD = sc.textFile(filePath).map{ record =>
       val splitRecord = record.split(",")
        val ageRange = splitRecord(0).toInt / 10 * 10
        val maleOrFemale = splitRecord(1)
        val point = splitRecord(2).toInt
        (ageRange, maleOrFemale, point)
      }

      questionnaireRDD.cache

      val avgAll = computeAllAvg(questionnaireRDD)
      val avgAgeRange = computeAgeRangeAvg(questionnaireRDD)

      val numMAcc = sc.accumulator(0, "Number of M")
      val totalPointMAcc = sc.accumulator(0, "TotalPoint of M")

      val numFAcc = sc.accumulator(0, "Number of M")
      val totalPointFAcc = sc.accumulator(0, "TotalPoint of F")

      val avgMorF = computeMorFAvg(questionnaireRDD, numMAcc, totalPointMAcc, numFAcc, totalPointFAcc)

      println(s"AVG ALL: $avgAgeRange")
      avgAgeRange.foreach{
        case (ageRange, avg) =>
          println(s"AVG Age Range ($ageRange): $avg")
      }

      avgMorF.foreach {
        case (mOrF, avg) =>
          println(s"AVG $mOrF: $avg")
      }
    } finally {
      sc.stop()
    }
  }

}
