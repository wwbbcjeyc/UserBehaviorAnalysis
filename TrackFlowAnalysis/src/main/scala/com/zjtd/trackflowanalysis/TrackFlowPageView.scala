package com.zjtd.trackflowanalysis

import java.text.SimpleDateFormat
import java.util.Properties


import com.zjtd.trackflowanalysis.bean.TrackFlowEvent
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema


object TrackFlowPageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "172.16.205.23:9092")
    properties.getProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream: DataStream[ObjectNode] = env.addSource(new FlinkKafkaConsumer("dwb-b_track_detail_info_i", new JSONKeyValueDeserializationSchema(true), properties))

    val dataStream: DataStream[TrackFlowEvent] = inputStream.map(data => {
      val formator = new SimpleDateFormat("yyyy-MM-dd")
      val date: String = formator.format(data.get("value").get("servertime").toString.toLong)
      TrackFlowEvent(date
        ,data.get("value").get("flow_id").toString
        ,data.get("value").get("eventsn").toString
        ,data.get("value").get("parent_uuid").toString
        ,data.get("value").get("udid").toString
        ,data.get("value").get("utm").toString
      )

    })

    dataStream.print()





    env.execute()



  }

}
