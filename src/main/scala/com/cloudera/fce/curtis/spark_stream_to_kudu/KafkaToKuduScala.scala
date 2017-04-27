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
    val kuduTableName = "impala::default.traffic_conditions"
    val kafkaBrokers  = args(0)
    val kuduMasters   = args(1)

    val sparkConf   = new SparkConf().setAppName("KafkaToKuduScala")
    val sc          = new SparkContext(sparkConf)
    val ssc         = new StreamingContext(sc, Seconds(5))
    val sqlContext  = new SQLContext(sc)

    val kuduContext = new KuduContext(kuduMasters) 

    val topicsSet       = Set("traffic") 
    val kafkaParams     = Map[String, String]("metadata.broker.list" -> kafkaBrokers) 

    val dstream         = KafkaUtils.createDirectStream[String, String, StringDecoder, 
                                                        StringDecoder](ssc, kafkaParams, topicsSet)
    val windowedStream  = dstream.window(Seconds(60))

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
						FROM traffic""")

       /* NOTE: All 3 methods provided  are equivalent UPSERT operations on the Kudu table and 
	        are idempotent, so we can run all 3 in this example (although only 1 is necessary) */

       // Method 1: All kudu operations can be used with KuduContext (INSERT, INSERT IGNORE, 
       //           UPSERT, UPDATE, DELETE) 
       kuduContext.upsertRows(resultsDataFrame, kuduTableName)

       // Method 2: The DataFrames API provides the 'write' function (results in a Kudu UPSERT) 
       val kuduOptions: Map[String, String] = Map("kudu.table"  -> kuduTableName,
       	                                          "kudu.master" -> kuduMasters)
       resultsDataFrame.write.options(kuduOptions).mode("append").kudu

       // Method 3: A SQL INSERT through SQLContext also results in a Kudu UPSERT
       resultsDataFrame.registerTempTable("traffic_results")
       sqlContext.read.options(kuduOptions).kudu.registerTempTable(kuduTableName)
       sqlContext.sql(s"INSERT INTO TABLE `$kuduTableName` SELECT * FROM traffic_results")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
