### Spark Streaming and Kudu Integration Sample Code

Programmatic implementations of the Cloudera Envelope [traffic sample](https://github.com/cloudera-labs/envelope/tree/master/examples/traffic) in Scala, Java and Python.

Tested with: CDH 5.13.1, Spark 2.1.0, Cloudera Kafka 3.0 (Apache 0.11.0), Kudu 1.5

#### To run the applications:

1. Build from the project root directory:

    ```
    mvn clean package
    ```
2. Create the target Kudu table using the Envelope traffic example [Impala DDL script](ddl/create_impala_kudu_table.sql)
3. Create the Kafka *traffic* topic (replication and partitions set to 1, for testing):

    ```
    /usr/bin/kafka-topics --create --zookeeper curtis-pa-2.vpc.cloudera.com:2181 --replication-factor 1 --topic traffic --partitions 1
    ```
4. Produce simulated data on the topic (replace the kafka broker/port list parameter):

    ```
    while true; do echo "`date +%s%N | cut -b1-13`,$((RANDOM % 100))"; sleep 1; done | /usr/bin/kafka-console-producer --broker-list curtis-pa-1:9092 --topic traffic
    ```
5. Run either the Scala, Java or Python Spark Streaming application (replace kafka brokers and kudu masters parameters):

    ```
    SPARK_KAFKA_VERSION=0.10 spark2-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduJava target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar kafka-broker-1:9092,... kudu-master-1:7051,...
    ```
    ```
    SPARK_KAFKA_VERSION=0.10 spark2-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduScala target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar kafka-broker-1:9092,... kudu-master-1:7051,...
    ```
    *PySpark:*  after building Scala/Java code in step 1, a kudu-spark_2...jar file should be available, typically under your *~/.m2* path
    ```
    spark2-submit --jars ~/.m2/repository/org/apache/kudu/kudu-spark2_2.11/1.5.0-cdh5.13.1/kudu-spark2_2.11-1.5.0-cdh5.13.1.jar src/main/python/kafka_to_kudu.py kafka-broker-1:9092,... kudu-master-1:7051    ,...
    ```
6. View the results in Kudu from Impala:

    ```
    select * from traffic_conditions order by as_of_time;
    ```
