package sparkcollection.fileread.csv

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 *  ./build.sh sparkcollection.fileread.csv.Sample1Main /home1/irteamsu/git/toy-spark/target/scala-park-sample-1.0-SNAPSHOT.jar
 */
object Sample1Main {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("/user/irteamsu/input/2015-summary.csv")

    println("result ->")
    flightData2015.groupBy("DEST_COUNTRY_NAME").count().show()
  }
}
