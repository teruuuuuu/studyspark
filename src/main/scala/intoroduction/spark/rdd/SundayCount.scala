package intoroduction.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeConstants}

/**
  * Created by arimuraterutoshi on 2017/08/25.
  */
object SundayCount {

  def main(args: Array[String]): Unit = {
    val filePath = "data/sundaycount.csv"
    val conf = new SparkConf().setAppName("SundayCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      val textRDD = sc.textFile(filePath)

      val dateTimeRDD = textRDD.map { dateStr =>
        val pattern =
          DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }

      val sundayRDD = dateTimeRDD.filter { dateTime =>
        dateTime.getDayOfWeek == DateTimeConstants.SUNDAY
      }

      val numOfSunday = sundayRDD.count()
      println(s"与えられたデータの中に日曜日は${numOfSunday}個含まれていました")
    } finally {
      sc.stop()
    }
  }

}
