### Spark Streaming -to- Kudu sample code and equivalent Envelope implementation

Programmatic implementations of the Cloudera Envelope project [traffic sample](https://github.com/cloudera-labs/envelope/tree/master/examples/traffic) functionality in Java and Scala, as an example of *Kafka --> Spark Streaming --> Kudu* Spark code, and comparison with Envelope's [configuration-only approach](https://github.com/cloudera-labs/envelope/blob/master/examples/traffic/traffic.conf) to implement the same thing.

Tested with: CDH 5.10 (Spark 1.6), Cloudera Kafka 2.1 (Apache 0.10), Kudu 1.2

1. Build from the project root:

    ```
    mvn clean package
    ```
2. Create the target Kudu table using the Envelope traffic example [Impala DDL script](https://github.com/cloudera-labs/envelope/blob/master/examples/traffic/create_traffic_conditions.sql)
3. Create the Kafka *traffic* topic (replication and partitions set to 1, for testing) and produce simulated data on the topic, **replacing the kafka broker/port list parameter**:

    ```
    /usr/bin/kafka-topics --create --zookeeper curtis-pa-2.vpc.cloudera.com:2181 --replication-factor 1 --topic traffic --partitions 1
    ```
    ```
    while true; do echo "`date +%s%N | cut -b1-13`,$((RANDOM % 100))"; sleep 1; done | /usr/bin/kafka-console-producer --broker-list curtis-pa-1:9092 --topic traffic
    ```
4. Run either the Scala or Java Spark Streaming application, **replacing the comma separated lists of kafka brokers and kudu masters**

    ```
    spark-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduJava target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar  kafka-broker-1:9092,... kudu-master-1:7051,...
    ```
    ```
    spark-submit --class com.cloudera.fce.curtis.spark_stream_to_kudu.KafkaToKuduScala target/spark_stream_to_kudu-1.0-jar-with-dependencies.jar kafka-broker-1:9092,... kudu-master-1:7051,...
    ```
5. View the resulting data in Kudu from Impala:

    ```
    select * from traffic_conditions order by as_of_time;
    ```
