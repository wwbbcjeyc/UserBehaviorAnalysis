package com.zitd.hotitemAnalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducerUtil {




  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }


  def writeToKafka(topic: String): Unit ={
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 创建kafka producer
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    // 读取文件数据，逐行发送到kafka topic
    val bufferSource: BufferedSource = io.Source.fromFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/HotItemAnalysis/src/main/resources/UserBehavior.csv")
    for( line <- bufferSource.getLines() ){
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }


}
