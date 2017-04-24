# spark_stream_to_kudu

while true; do echo "`date +%s%N | cut -b1-13`,$((RANDOM % 100))"; sleep 1; done | /usr/bin/kafka-console-producer --broker-list curtis-pa-1:9092 --topic traffic

spark-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduScala target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar curtis-pa-1:9092 traffic
spark-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduJava target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar
