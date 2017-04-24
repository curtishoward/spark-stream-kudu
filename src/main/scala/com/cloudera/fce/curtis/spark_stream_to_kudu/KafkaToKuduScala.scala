package com.cloudera.fce.curtis.spark_stream_to_kudu

import kafka.serializer.StringDecoder
import org.apache.spark.sql.types._                         // ?
import org.apache.spark.sql.{DataFrame, Row, SQLContext}  // ?
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._  // ?
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._

/**
 *
 *
 * Example:
 *    $
 */
object KafkaToKuduScala {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("KafkaToKuduScala")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(5))
    val kuduMasters = Seq("curtis-pa-2:7051").mkString(",")
    val kuduContext = new KuduContext(kuduMasters)

    val topics: String = "traffic"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "curtis-pa-2:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val windowedMessages = messages.window(Seconds(60))

    windowedMessages.foreachRDD { rdd =>
       import sqlContext.implicits._
       val dataFrame = rdd.map(_._2).map(rec => (rec.split(",")(0).toLong,rec.split(",")(1).toInt)).toDF("measurement_time","number_of_vehicles")
       dataFrame.registerTempTable("traffic")
       val resultsDF = sqlContext.sql("SELECT UNIX_TIMESTAMP() * 1000 as_of_time, ROUND(AVG(number_of_vehicles), 2) avg_num_veh, MIN(number_of_vehicles) min_num_veh, MAX(number_of_vehicles) max_num_veh, MIN(measurement_time) first_meas_time, MAX(measurement_time) last_meas_time FROM traffic")
       //resultsDF.show()
       kuduContext.upsertRows(resultsDF, "impala::default.traffic_conditions")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
