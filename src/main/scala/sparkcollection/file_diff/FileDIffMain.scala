package sparkcollection.file_diff

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object FileDIffMain {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val beforeRdd = sc.textFile("/user/irteamsu/input/ad/before")
    beforeRdd.toDF();
    val beforeDf = beforeRdd.toDF()

    val afterRdd = sc.textFile("/user/irteamsu/input/ad/after")
    afterRdd.toDF();
    val afterDf = afterRdd.toDF()

    val diffDF = afterDF.except(beforeDF)
    diffDF.write.csv("/user/irteamsu/input/ad/diff")
  }
}
