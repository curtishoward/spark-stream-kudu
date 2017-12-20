import sys

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_to_kudu.py <kafka-brokers> <kudu-masters>")
        exit(-1)

    kuduTableName = "impala::default.traffic_conditions"
    kafkaBrokers, kuduMasters = sys.argv[1:]
    topicSet =  ["traffic"]

    spark = SparkSession.builder.appName("KafkaToKuduPython").getOrCreate()
    ssc = StreamingContext(spark.sparkContext, 5)

    dstream = KafkaUtils.createDirectStream(ssc, topicSet, {"metadata.broker.list": kafkaBrokers})
    windowedStream = dstream.window(60)

    def process(time, rdd):
      if rdd.isEmpty() == False:

        dataFrame = rdd.map(lambda rec: (long(rec[1].split(",")[0]), int(rec[1].split(",")[1].rstrip())))\
                       .toDF(["measurement_time","number_of_vehicles"])
        castDF = dataFrame.withColumn("number_of_vehicles_int", dataFrame.number_of_vehicles.cast("int"))\
                 .drop("number_of_vehicles")

        castDF.registerTempTable("traffic")
      
        resultsDF = spark.sql(""" SELECT UNIX_TIMESTAMP() * 1000 as_of_time, 
                                              ROUND(AVG(number_of_vehicles_int), 2) avg_num_veh,
   				              MIN(number_of_vehicles_int) min_num_veh, 
			  	              MAX(number_of_vehicles_int) max_num_veh,
	                                      MIN(measurement_time) first_meas_time, 
					      MAX(measurement_time) last_meas_time  
                                       FROM traffic""")

        resultsDF.write.format('org.apache.kudu.spark.kudu').option('kudu.master',kuduMasters)\
                 .option('kudu.table',kuduTableName).mode("append").save()

    windowedStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
