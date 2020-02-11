package sparkcollection.acumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object AccumulatorMain {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("acumulator")
    val sc = new SparkContext(conf)

    val inputPath = args(0) //hdfs://10.105.197.53:8020/user/irteamsu/input/statePopulation.csv

    val statePopulationAcc = new StateAccumulator
    sc.register(statePopulationAcc, "statePopulationAcc")

    val statesPopulationRDD = sc.textFile(inputPath).filter(_.split(",")(0) != "State")
    statesPopulationRDD.take(10)

    statesPopulationRDD.map(x => {
      val toks = x.split(",")
      val year = toks(1).toInt
      val pop = toks(2).toLong
      statePopulationAcc.add(YearPopulation(year, pop))
      x
    }).count()

    print("[value] : "+statePopulationAcc.value)
  }
}
