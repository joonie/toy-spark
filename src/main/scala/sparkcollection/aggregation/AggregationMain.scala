package sparkcollection.aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object AggregationMain {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName("aggregation")
      .config(conf)
      .getOrCreate()

    val inputPath = args(0)

    val statesDF = spark.read.option("header", "true").option("inferschema", "true").option("sep", ",").csv(inputPath)
    statesDF.show(5)
  }
}
