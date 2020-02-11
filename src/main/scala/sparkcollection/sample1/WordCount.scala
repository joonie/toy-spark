package sparkcollection.sample1

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object WordCount {
  def main(args: Array[String]) : Unit = {
    val inputPath = args(0) //hdfs://10.105.197.53:8020/user/irteamsu/input/statePopulation.csv
    val outputPath = args(1) //hdfs://10.105.197.53:8020/user/irteamsu/input/statePopulation_output.csv

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val statesPopulationRDD = sc.textFile(inputPath)
    statesPopulationRDD.saveAsTextFile(outputPath)
  }
}
