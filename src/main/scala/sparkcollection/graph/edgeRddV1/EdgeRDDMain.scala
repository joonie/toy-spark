package sparkcollection.graph.edgeRddV1

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object EdgeRDDMain {
  def main(agrs: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EdgeRDDMain")
    val sc = new SparkContext(conf)

    val users = sc.textFile("hdfs://10.105.197.53:8020/user/irteamsu/input/user.txt")
      .map { line =>
        val fields = line.split(",")
        (fields(0).toLong, User(fields(1), fields(2)))
      }

    println(users.take(10))
  }
}
