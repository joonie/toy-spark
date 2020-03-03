package sparkcollection.nested

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 * ./build.sh sparkcollection.nested.NestedDataSetExample /user/irteamsu/input/json_sample1
 */
object NestedDataSetExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName("NestedDataSet")
      .config(conf)
      .getOrCreate()

    val jsonPath = args(0)
//    val jsonPath = "/user/irteamsu/input/NestedJson.json"

    import spark.implicits._

    val jsonDf = spark.read.json(jsonPath)
    jsonDf.foreach(row => {
      val group = row.getAs("group")
      println("row : " + row + ", group : "+group)
    })

    val dataset = spark.read.json(jsonPath).as[GroupBean]
    dataset.foreach(row => {
      val group = row.group
      val time = row.time
      println("dataframe : " + row + ", group : " + group + ", time : " + time)
    })

    dataset.printSchema()

    dataset.toDF().createOrReplaceTempView("case_table")
    spark.sql("select * from case_table").rdd.foreach(println)

    spark.stop()
  }

  case class GroupBean(group: String, time: Long, value: Long, nested: Array[NestedBean])

  case class NestedBean(col1: Long, col2: Long)
}
