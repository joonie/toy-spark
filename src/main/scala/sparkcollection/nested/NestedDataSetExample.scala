package sparkcollection.nested

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 * ./build.sh sparkcollection.nested.NestedDataSetExample /user/irteamsu/input/NestedJson.json
 *
 * Collect
 * Return all the elements of the dataset as an array at the driver program.
 * This is usually useful after a filter or other operation that returns a sufficiently small subset of the data.
 *
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

    /**
     * row : [A,WrappedArray([0.1,0.2], [1.1,1.2]),5,3], group : A
     * row : [B,WrappedArray([1.0,2.0], [1.0,2.0]),5,3], group : B
     * row : [C,WrappedArray([1.0,2.0], [1.0,2.0]),5,3], group : C
     */
    val jsonDf = spark.read.json(jsonPath)
    jsonDf.collect().foreach(row => {
      val group = row.getAs[String]("group")
      println("row : " + row + ", group : "+group)
    })

    /**
     * dataframe : GroupBean(A,5.0,3.0,[L$line45.$read$$iw$$iw$NestedBean;@4274af83), group : A, time : 5.0
     * dataframe : GroupBean(B,5.0,3.0,[L$line45.$read$$iw$$iw$NestedBean;@443ed4f0), group : B, time : 5.0
     * dataframe : GroupBean(C,5.0,3.0,[L$line45.$read$$iw$$iw$NestedBean;@6383b7da), group : C, time : 5.0
     */
    val dataset = spark.read.json(jsonPath).as[GroupBean]
    dataset.collect().foreach(row => {
      val group = row.group
      val time = row.time
      println("dataframe : " + row + ", group : " + group + ", time : " + time)
    })

    /**
     * root
     * |-- group: string (nullable = true)
     * |-- nested: array (nullable = true)
     * |    |-- element: struct (containsNull = true)
     * |    |    |-- col1: double (nullable = true)
     * |    |    |-- col2: double (nullable = true)
     * |-- time: long (nullable = true)
     * |-- value: long (nullable = true)
     */
    dataset.printSchema()

    /**
     * [A,WrappedArray([0.1,0.2], [1.1,1.2]),5,3]
     * [B,WrappedArray([1.0,2.0], [1.0,2.0]),5,3]
     * [C,WrappedArray([1.0,2.0], [1.0,2.0]),5,3]
     */
    dataset.toDF().createOrReplaceTempView("case_table")
    spark.sql("select * from case_table").rdd.collect().foreach(println)

    spark.stop()
  }

  case class GroupBean(group: String, time: Double, value: Double, nested: Array[NestedBean])

  case class NestedBean(col1: Double, col2: Double)
}
