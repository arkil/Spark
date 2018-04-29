KAFKA -- SPARKSTREAMING -- PARQUET


* Create kafka topic :

kafka-topics --create --zookeeper <zookeepr name> --replication-factor 2 --partition 3 --topic topicA --config retention.ms 10000 

* Create Kafka producer :

kafka-console-producer --broker-list <list of broker> -- topicA 

--> Pass messages in JSON format 


* Run jar on server

spark2-submit --class com.org.spark.App SparkStreamingKafka-1.0-SNAPSHOT.jar 

* To see the Output 

hdfs dfs -cat output.parquet