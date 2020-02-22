package sparkcollection.stream.basic1

import org.apache.spark.SparkConf
import org.apache.spark.streaming._


/**
 * @author 손영준 (youngjun.son@navercorp.com)
 *
 * ./build.sh sparkcollection.stream.basic1.StreamBasicV1
 * (중요) 스트림의 경우 신규파일만 영향을 받음으로 테스트 시 해당 경로에 파일을 넣어줘야한다.
 */
object StreamBasicV1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("StreamBasicV1")
    val ssc = new StreamingContext(conf, Seconds(10))

    //디렉토리 경로
    val fileStream = ssc.textFileStream("hdfs://dev-shop-collection-nn-ncl.nfra.io:8020/user/irteamsu/input/stream")

    fileStream.foreachRDD(rdd => println("[result] : "+rdd.count()))

    //스트림 시작
    ssc.start()
    ssc.awaitTermination()

    //false : 즉시 중지, (ture, ture)를 넣으면 모든 데이터가 처리되었는지 확인
    //  ssc.stop(false)
  }
}
