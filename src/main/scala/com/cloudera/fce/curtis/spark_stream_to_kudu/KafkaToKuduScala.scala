package com.cloudera.fce.curtis.spark_stream_to_kudu

import kafka.serializer.StringDecoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: DirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2
 */
object KafkaToKuduScala {
  def main(args: Array[String]) {
    //if (args.length < 3) {
    //  System.err.println("KafkaWordCount <brokers> <topic>")
    //  System.exit(1)
    //}

    // StreamingExamples.setStreamingLogLevels()

    //val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(5))
    val kuduMasters = Seq("curtis-pa-2:7051").mkString(",")
    val kuduContext = new KuduContext(kuduMasters)

    val topics: String = "traffic"
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "curtis-pa-2:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val windowedMessages = messages.window(Seconds(60))


    windowedMessages.foreachRDD { rdd =>
       println("Hello")
       import sqlContext.implicits._
       val dataFrame = rdd.map(_._2).map(rec => (rec.split(",")(0).toLong,rec.split(",")(1).toInt)).toDF("measurement_time","number_of_vehicles")
       dataFrame.registerTempTable("traffic")
       val resultsDF = sqlContext.sql("SELECT UNIX_TIMESTAMP() * 1000 as_of_time, ROUND(AVG(number_of_vehicles), 2) avg_num_veh, MIN(number_of_vehicles) min_num_veh, MAX(number_of_vehicles) max_num_veh, MIN(measurement_time) first_meas_time, MAX(measurement_time) last_meas_time FROM traffic")
       resultsDF.show()
       kuduContext.upsertRows(resultsDF, "impala::default.traffic_conditions")
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
