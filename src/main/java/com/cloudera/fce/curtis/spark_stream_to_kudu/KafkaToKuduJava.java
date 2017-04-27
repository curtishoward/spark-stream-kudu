package com.cloudera.fce.curtis.spark_stream_to_kudu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.kafka.KafkaUtils;  
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField; 
import org.apache.spark.sql.types.StructType;  
import org.apache.spark.sql.types.DataTypes;    
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.HashMap; 
import java.util.HashSet; 
import java.util.Arrays;  
import java.util.Map;
import java.util.Set;

import org.apache.kudu.spark.kudu.KuduContext;

import kafka.serializer.StringDecoder;

import java.util.HashMap; 
import java.util.HashSet; 
import java.util.Arrays;  
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

// Usage:  KafkaToKuduJava [kafka-brokers] [kudu-masters-comma-separated]

public class KafkaToKuduJava {
    
    public static void main(String[] args) throws Exception {
        final String kuduTableName      = "impala::default.traffic_conditions";
        final String kafkaBrokers       = args[0];
	final String kuduMasters        = args[1];

	SparkConf sparkConf             = new SparkConf().setAppName("KafkaToKuduJava"); 
        JavaSparkContext sc             = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc        = new JavaStreamingContext(sc, new Duration(5000));
        final SQLContext sqlContext     = new SQLContext(sc);

	final KuduContext kuduContext   = new KuduContext(kuduMasters);   

        Set<String> topicsSet           = new HashSet<String>(Arrays.asList("traffic"));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBrokers);

        JavaPairDStream<String, String> dstream = KafkaUtils.createDirectStream(
                ssc, String.class, String.class, StringDecoder.class, 
		StringDecoder.class, kafkaParams, topicsSet);

	JavaPairDStream<String, String> windowedStream = dstream.window(new Duration(60000));

        windowedStream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {

            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {

                JavaRDD<Row> fieldsRdd = rdd.map(new Function<Tuple2<String,String>, Row>() {

                    @Override
                    public Row call(Tuple2<String, String> rec) {
                        String[] flds    = rec._2().split(",");
		        Long measure     = Long.parseLong(flds[0]);
		        Integer vehicles = Integer.parseInt(flds[1].trim());

     		        return RowFactory.create(measure,vehicles);
                    }
                });
              
                StructType schema = DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("measurement_time", DataTypes.LongType, false),
                    DataTypes.createStructField("number_of_vehicles", DataTypes.IntegerType, true)});

                DataFrame dataFrame = sqlContext.createDataFrame(fieldsRdd,schema);
                dataFrame.registerTempTable("traffic");

		String query = "SELECT UNIX_TIMESTAMP() * 1000 as_of_time,"           +
			       "       ROUND(AVG(number_of_vehicles),2) avg_num_veh," +
			       "       MIN(number_of_vehicles) min_num_veh,"          +
                               "       MAX(number_of_vehicles) max_num_veh,"          +
			       "       MIN(measurement_time) first_meas_time,"        +
                               "       MAX(measurement_time) last_meas_time "         +
			       "FROM traffic";
                DataFrame resultsDataFrame = sqlContext.sql(query);

                /* NOTE: All 3 methods provided are equivalent UPSERT operations on the 
		         Kudu table and are idempotent, so we can run all 3 in this example 
			 (although only 1 is necessary) */

                // Method 1: All kudu operations can be used with KuduContext (INSERT, INSERT IGNORE, 
		//           UPSERT, UPDATE, DELETE) 
		kuduContext.upsertRows(resultsDataFrame, kuduTableName);

                // Method 2: The DataFrames API  provides the 'write' function (results 
		//           in a Kudu UPSERT) 
                final Map<String, String> kuduOptions = new HashMap<>();
                kuduOptions.put("kudu.table",  kuduTableName);
                kuduOptions.put("kudu.master", kuduMasters);
                resultsDataFrame.write().format("org.apache.kudu.spark.kudu")
			        .options(kuduOptions).mode("append").save();

                // Method 3: A SQL INSERT through SQLContext also results in a Kudu UPSERT 
                resultsDataFrame.registerTempTable("traffic_results");
                sqlContext.read().format("org.apache.kudu.spark.kudu").options(kuduOptions).load()
			  .registerTempTable(kuduTableName);
                sqlContext.sql("INSERT INTO TABLE `" + kuduTableName + "` SELECT * FROM traffic_results");

		return null;
            }
        });
        
        ssc.start();
        ssc.awaitTermination();
    }
}
