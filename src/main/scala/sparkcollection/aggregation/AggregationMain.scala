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

    //column이 State인 데이터 개수 출력
    statesDF.select(col("*")).agg(count("State")).show
    statesDF.select(count("State")).show

    //column이 중복 제외하고 State 개수 출력
    statesDF.select(col("*")).agg(countDistinct("State")).show
    statesDF.select(countDistinct("State")).show

    //State의 첫번째 값 출력
    statesDF.select(first("State")).show //Alabama

    //State의 마지막 값 출력
    statesDF.select(last("State")).show //Wyoming

    //colume이 State인 것들의 대략적인 개수를 구한다
    statesDF.select(col("*")).agg(approx_count_distinct("State")).show //48
    statesDF.select(col("*")).agg(approx_count_distinct("State", 0.2)).show//49

    //Population Column에 대해서 최대 최소
    statesDF.select(min("Population")).show
    statesDF.select(max("Population")).show

    //avg, sum, sumDistinct
    statesDF.select(avg("Population")).show
    statesDF.select(sum("Population")).show
    statesDF.select(sumDistinct("Population")).show

    //첨도 : 분포의 중간과 끝의 가중치
    statesDF.select(kurtosis("Population")).show

    //비대칭도 : 평균 근처의 데이터 값에 대한 비대칭 측정
    statesDF.select(skewness("Population")).show

    //분산 : 개별값에서 평균의 차르 제곱한 후의 평균
    statesDF.select(var_pop("Population")).show

    //표준편차
    statesDF.select(stddev("Population")).show

    //공분산 (두 랜덤 변수의 결합 가변성을 측정)
    statesDF.select(covar_pop("Year", "Population")).show
  }
}
