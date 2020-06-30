package sparkcollection.fileread.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object Sample1Main {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("sample1")
    val spark = SparkSession
      .builder()
      .appName("fileDiffMain")
      .config(conf)
      .getOrCreate()

    val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("/user/irteamsu/input/2015-summary.csv")

    print("[result]")
    flightData2015.take(3)
    print(flightData2015.take(3))

  }
}
