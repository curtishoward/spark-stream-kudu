package com.cloudera.fce.curtis.spark_stream_to_kudu

import org.apache.spark.streaming._
import org.apache.spark.SparkConf

object SocketToKudu {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SocketToKudu")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("127.0.0.1", 12345)
    val lengthAndCount = lines.map(line => (line.length(),1)).reduce((val1,val2) => ((val1._1 + val2._1),(val1._2 + val2._2))) 
    println(lengthAndCount)
    
    ssc.start()
    ssc.awaitTermination()
  }
}
