package com.cloudera.fce.curtis.spark_stream_to_kudu

import org.apache.spark.sql.SparkSession  
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._  
import org.apache.spark.streaming._
import org.apache.kudu.spark.kudu._

// Usage:  KafkaToKuduScala [kafka-brokers] [kudu-masters] 

object KafkaToKuduScala {
  def main(args: Array[String]) {
    val kuduTableName = "traffic_conditions"
    val kafkaBrokers  = args(0)
    val kuduMasters   = args(1)

    val sparkConf   = new SparkConf().setAppName("KafkaToKuduScala")
    val spark       = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc         = new StreamingContext(spark.sparkContext, Seconds(5))
    val kuduContext = new KuduContext(kuduMasters, spark.sparkContext)

    val topicsSet       = Set("traffic") 
    val kafkaParams     = Map[String, String]("bootstrap.servers"  -> kafkaBrokers,
                                              "group.id"           -> "curtis_test_group",
                                              "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
                                              "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer") 

    val dstream         = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, 
                                     ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val windowedStream  = dstream.window(Seconds(60))

    windowedStream.foreachRDD { rdd =>
       import spark.implicits._

       val dataFrame = rdd.map(rec => (rec.value.split(",")(0).toLong,rec.value.split(",")(1).toInt))
			  .toDF("measurement_time","number_of_vehicles")
       dataFrame.registerTempTable("traffic")

       val resultsDataFrame = spark.sql("""SELECT UNIX_TIMESTAMP() * 1000 as_of_time, 
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
       spark.read.options(kuduOptions).kudu.registerTempTable(kuduTableName)
       spark.sql(s"INSERT INTO TABLE $kuduTableName SELECT * FROM traffic_results")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
