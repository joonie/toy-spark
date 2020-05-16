package sparkcollection.file_diff

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import org.apache.commons.cli.{CommandLine, HelpFormatter, Options, PosixParser}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
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

    /*val currentDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
    println(currentDate)

    val beforeDate = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
    println(beforeDate)

    val before = spark.read.csv("/user/irteamsu/input/ad/before")
    val after = spark.read.csv("/user/irteamsu/input/ad/after")

    val diff = after.except(before)
    println(diff.count)

    diff.write.csv("/user/irteamsu/input/ad/diff/"+currentDate)*/

    val currentDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
    println(currentDate)

    val beforeDate = LocalDateTime.now().minusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMddHH"))
    println(beforeDate)

    val before = spark.read.csv(conf.get("beforePath"))
    val after = spark.read.csv(conf.get("afterPath"))

    val diff = after.except(before)
    println(diff.count)

    diff.write.csv(conf.get("destPath")+currentDate)
  }

  def getConf(args: Array[String]) = {
    val conf = new Configuration
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs()

    val cmd = parseArgs(otherArgs)
    val beforePath = cmd.getOptionValue("beforePath")
    val afterPath = cmd.getOptionValue("afterPath")
    val destPath = cmd.getOptionValue("destPath")

    conf.set("beforePath", beforePath)
    conf.set("afterPath", afterPath)
    conf.set("destPath", destPath)
    conf
  }

  def parseArgs(args: Array[String]): CommandLine = {
    val options = new Options()

    var cmd: CommandLine = null
    try {
      val parser = new PosixParser()
      cmd = parser.parse(options, args)
    } catch {
      case e: Throwable => {
        println("ERROR: " + e.getMessage())
        val formatter = new HelpFormatter()
        formatter.printHelp("Application ", options, true)
        sys.exit(-1)
      }
    }
    cmd
  }
}
