### Spark Streaming-to-Kudu sample code (and code-as-a-config using Envelope)

Programmatic implementations of the Cloudera Envelope [traffic sample](https://github.com/cloudera-labs/envelope/tree/master/examples/traffic), demonstrating Spark Streaming code writing to Kudu, versus Envelope's [configuration-only approach](https://github.com/cloudera-labs/envelope/blob/master/examples/traffic/traffic.conf).

Tested with: CDH 5.10 (Spark 1.6), Cloudera Kafka 2.1 (Apache 0.10), Kudu 1.2

1. Build from the project root directory:

    ```
    mvn clean package
    ```
2. Create the target Kudu table using the Envelope traffic example [Impala DDL script](https://github.com/cloudera-labs/envelope/blob/master/examples/traffic/create_traffic_conditions.sql)
3. Create the Kafka *traffic* topic (replication and partitions set to 1, for testing):

    ```
    /usr/bin/kafka-topics --create --zookeeper curtis-pa-2.vpc.cloudera.com:2181 --replication-factor 1 --topic traffic --partitions 1
    ```
4. Produce simulated data on the topic (replace the kafka broker/port list parameter):
    ```
    while true; do echo "`date +%s%N | cut -b1-13`,$((RANDOM % 100))"; sleep 1; done | /usr/bin/kafka-console-producer --broker-list curtis-pa-1:9092 --topic traffic
    ```
4. Run either the Scala, Java or Python Spark Streaming application (replace kafka brokers and kudu masters parameters):

    ```
    spark-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduJava target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar  kafka-broker-1:9092,... kudu-master-1:7051,...
    ```
    ```
    spark-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduScala target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar kafka-broker-1:9092,... kudu-master-1:7051,...
    ```
    *PySpark:*  after building Scala/Java code in step 1, a kudu-spark_2...jar file should be available, typically under your *~/.m2* path
    ```
    spark-submit --jars /var/lib/hadoop-hdfs/.m2/repository/org/apache/kudu/kudu-spark_2.10/1.2.0-cdh5.10.0/kudu-spark_2.10-1.2.0-cdh5.10.0.jar src/main/python/kafka_to_kudu.py kafka-broker-1:9092,... kudu-master-1:7051    ,...
    ```
5. View the results in Kudu from Impala:

    ```
    select * from traffic_conditions order by as_of_time;
    ```
