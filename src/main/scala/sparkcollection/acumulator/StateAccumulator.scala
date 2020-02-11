package sparkcollection.acumulator

import org.apache.spark.util.AccumulatorV2

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
class StateAccumulator extends AccumulatorV2[YearPopulation, YearPopulation] {
  //declare the two variables one Int for year and Long for population
  private var year = 0
  private var population:Long = 0L

  //return iszero if year and population are zero
  override def isZero: Boolean = year == 0 && population == 0L

  //copy accumulator and return a new accumulator
  override def copy(): StateAccumulator = {
    val newAcc = new StateAccumulator
    newAcc.year =     this.year
    newAcc.population = this.population
    newAcc
  }

  //reset the year and population to zero
  override def reset(): Unit = { year = 0 ; population = 0L }

  //add a value to the accumulator
  override def add(v: YearPopulation): Unit = {
    year += v.year
    population += v.population
  }

  //merge two accumulators
  override def merge(other: AccumulatorV2[YearPopulation, YearPopulation]): Unit = {
    other match {
      case o: StateAccumulator => {
        year += o.year
        population += o.population
      }
      case _ =>
    }
  }


  //function called by Spark to access the value of accumulator
  override def value: YearPopulation = YearPopulation(year, population)
}
