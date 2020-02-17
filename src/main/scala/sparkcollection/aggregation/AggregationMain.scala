package sparkcollection.aggregation

import org.apache.spark.sql.{SparkSession, TypedColumn}
import org.apache.spark.{SparkConf}
import org.apache.spark.sql.functions._

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

    statesDF.select(col("*")).agg(count("State")).show
    statesDF.select(count("State")).show

    statesDF.select(col("*")).agg(countDistinct("State")).show
    statesDF.select(countDistinct("State")).show
  }
}
