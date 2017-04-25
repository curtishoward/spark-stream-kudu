package com.cloudera.fce.curtis.spark_stream_to_kudu

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkContext, SparkConf}  
import org.apache.spark.sql.SQLContext  
import org.apache.spark.streaming.kafka._  
import org.apache.spark.streaming._
import org.apache.kudu.spark.kudu._

// Usage:  KafkaToKuduScala [kafka-brokers] [kudu-masters] 

object KafkaToKuduScala {
  def main(args: Array[String]) {

    val sparkConf   = new SparkConf().setAppName("KafkaToKuduScala")
    val sc          = new SparkContext(sparkConf)
    val ssc         = new StreamingContext(sc, Seconds(5))
    val sqlContext  = new SQLContext(sc)
    // Initialized our Kudu context with a comma separated list of masters
    val kuduContext = new KuduContext(args(1)) 

    val topicsSet       = Set("traffic") 
    val kafkaParams     = Map[String, String]("metadata.broker.list" -> args(0))

    val dstream         = KafkaUtils.createDirectStream[String, String, StringDecoder, 
                                                        StringDecoder](ssc, kafkaParams, topicsSet)
    val windowedStream = dstream.window(Seconds(60))

    windowedStream.foreachRDD { rdd =>
       import sqlContext.implicits._

       val dataFrame = rdd.map(rec => (rec._2.split(",")(0).toLong,rec._2.split(",")(1).toInt))
			  .toDF("measurement_time","number_of_vehicles")
       dataFrame.registerTempTable("traffic")

       val resultsDataFrame = sqlContext.sql("""SELECT UNIX_TIMESTAMP() * 1000 as_of_time, 
	                                               ROUND(AVG(number_of_vehicles), 2) avg_num_veh, 
						       MIN(number_of_vehicles) min_num_veh, 
						       MAX(number_of_vehicles) max_num_veh, 
				                       MIN(measurement_time) first_meas_time, 
						       MAX(measurement_time) last_meas_time 
						FROM traffic"""
		                            )

       // KuduContext allows us to apply a dataframe as a batch operation (e.g. upstert) on the Kudu table
       kuduContext.upsertRows(resultsDataFrame, "impala::default.traffic_conditions")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
