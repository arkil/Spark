package com.org.spark


import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}



/**
 * Hello world!
 *
 */
object App {

  def main(args: Array[String]): Unit = {

    println("Hello World!")

    val conf = new SparkConf().setAppName("KafkaSparkStreaming")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ssc = new StreamingContext(sc, Seconds(5))

    val props: Properties = new Properties()

    props.put(ConsumerConfig.GROUP_ID_CONFIG, "cosnumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
  }



}
