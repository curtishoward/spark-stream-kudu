package com.cloudera.fce.curtis.spark_stream_to_kudu;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes; 
import org.apache.spark.sql.types.StructField; 
import org.apache.spark.sql.types.StructType; 
import org.apache.spark.sql.RowFactory;

import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
//import org.apache.kudu.ColumnSchema;
//import org.apache.kudu.client.KuduClient;
//import org.apache.kudu.client.KuduException;
//import org.apache.kudu.client.KuduPredicate;
//import org.apache.kudu.client.KuduScanner;
//import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
//import org.apache.kudu.client.KuduSession;
//import org.apache.kudu.client.KuduTable;
//import org.apache.kudu.client.Operation;
//import org.apache.kudu.client.PartialRow;
//import org.apache.kudu.client.RowError;
//import org.apache.kudu.client.RowResult;
//import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.spark.kudu.KuduContext;


//import org.apache.kudu.client.Insert;

public class KafkaToKuduJava {
    
    //@SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {
        String brokersArgument = "curtis-pa-1:9092,curtis-pa-2:9092";
        String topicsArgument = "traffic";
        final String kuduConnectionArgument = "curtis-pa-2:7051";
        final String kuduTableArgument = "impala::default.traffic_conditions";
      
        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(5000));
        SQLContext sqlContext = new SQLContext(sc);
        KuduContext kuduContext = new KuduContext("curtis-pa-1.vpc.cloudera.com:7051");   

        Map<String, String> params = new HashMap<>();
        params.put("metadata.broker.list", brokersArgument);
        Set<String> topics = new HashSet<>(Arrays.asList(topicsArgument.split(","))); 
        
        JavaPairDStream<String, String> dstream = KafkaUtils.createDirectStream(
                ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, params, topics);
        JavaPairDStream<String, String> windowedStream = dstream.window(new Duration(60000));

        windowedStream.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> rdd) throws Exception {
                // System.out.println(batch.collect());
	        // Need to convert RDD to tuple then to DF then upsert into kudu
                //JavaRDD<Tuple2<Long,Integer>> fieldsRdd = rdd.map(new Function<Tuple2<String,String>, Tuple2<Long,Integer>>() {
                JavaRDD<Row> fieldsRdd = rdd.map(new Function<Tuple2<String,String>, Row>() {
                    @Override
                    public Row call(Tuple2<String, String> kafkaMessage) {
                    //public Tuple2<Long, Integer> call(Tuple2<String, String> kafkaMessage) {
                       //return new Tuple2<Long, Integer>(Long.parseLong(kafkaMessage._2().split(",")[0]),
                       //                                 Integer.parseInt(kafkaMessage._2().split(",")[1]));
                       return RowFactory.create(Long.parseLong(kafkaMessage._2().split(",")[0]), Long.parseLong(kafkaMessage._2().split(",")[1].trim()));
                    }
                });
              
		StructType schema = DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("measurement_time", DataTypes.LongType, false),
                    DataTypes.createStructField("number_of_vehicles", DataTypes.IntegerType, true)});
                DataFrame df = sqlContext.createDataFrame(fieldsRdd,schema);
                //DataFrame df = sqlContext.createDataFrame(rdd,schema);
                df.show();
                //df.show(); 
                return null;
            } 
        });
        
        ssc.start();
        ssc.awaitTermination();
    }
    
}
