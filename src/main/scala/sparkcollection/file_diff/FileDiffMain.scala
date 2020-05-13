package sparkcollection.file_diff

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 손영준 (youngjun.son@navercorp.com)
 */
object FileDiffMain {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    val spark = SparkSession
      .builder()
      .appName("fileDiffMain")
      .config(conf)
      .getOrCreate()

    val currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
    println(currentDate)

    val beforeDate = LocalDate.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
    println(beforeDate)

    val before = spark.read.csv("/user/irteamsu/input/ad/before")
    val after = spark.read.csv("/user/irteamsu/input/ad/after")

    val diff = after.except(before)
    println(diff.count)

    diff.write.csv("/user/irteamsu/input/ad/diff/"+currentDate)
  }
}
