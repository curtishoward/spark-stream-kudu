import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_to_kudu.py <kafka-brokers> <kudu-masters>")
        exit(-1)

    kuduTableName = "impala::default.traffic_conditions"
    kafkaBrokers, kuduMasters = sys.argv[1:]
    topicSet =  ["traffic"]

    sc = SparkContext(appName="KafkaToKuduPython")
    ssc = StreamingContext(sc, 5)
    sqlContext = SQLContext(sc)

    dstream = KafkaUtils.createDirectStream(ssc, topicSet, {"metadata.broker.list": kafkaBrokers})
    windowedStream = dstream.window(60)

    def process(time, rdd):
      if rdd.isEmpty() == False:

        dataFrame = rdd.map(lambda rec: (long(rec[1].split(",")[0]), int(rec[1].split(",")[1].rstrip())))\
                       .toDF(["measurement_time","number_of_vehicles"])
        # TODO: Understand why num vehicles interpreted as 'long' and not 'int' by toDF() 
	#       (cast to 'int' for now so that min/max_num_veh fields align with Kudu table schema)
        castDF = dataFrame.withColumn("number_of_vehicles_int", dataFrame.number_of_vehicles.cast("int"))\
                 .drop("number_of_vehicles")

        castDF.registerTempTable("traffic")
      
        resultsDF = sqlContext.sql(""" SELECT UNIX_TIMESTAMP() * 1000 as_of_time, 
                                              ROUND(AVG(number_of_vehicles_int), 2) avg_num_veh,
   				              MIN(number_of_vehicles_int) min_num_veh, 
			  	              MAX(number_of_vehicles_int) max_num_veh,
	                                      MIN(measurement_time) first_meas_time, 
					      MAX(measurement_time) last_meas_time  
                                       FROM traffic""")

        # NOTE:  The 2 methods below are equivalent UPSERT operations on the Kudu table and 
	#        are idempotent, so we can run both in this example (although only 1 is necessary) 

        # Method 1: Use DataFrames API allows provides the 'write' function (results in Kudu upsert) 
        resultsDF.write.format('org.apache.kudu.spark.kudu').option('kudu.master',kuduMasters)\
                 .option('kudu.table',kuduTableName).mode("append").save()

        # Method 2: A SQL INSERT through SQLContext also results in a Kudu Upsert
        resultsDF.registerTempTable("traffic_results")
	sqlContext.read.format('org.apache.kudu.spark.kudu').option('kudu.master',kuduMasters)\
                  .option('kudu.table',kuduTableName).load().registerTempTable(kuduTableName)
        sqlContext.sql("INSERT INTO TABLE `" + kuduTableName + "` SELECT * FROM traffic_results")

        # PySpark KuduContext not yet available (https://issues.apache.org/jira/browse/KUDU-1603) 

    windowedStream.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
