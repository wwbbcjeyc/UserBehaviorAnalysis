package com.zitd.hotitemAnalysis

import java.util.Properties

import com.zitd.hotitemAnalysis.aggregate.CountAgg
import com.zitd.hotitemAnalysis.bean.{ItemViewCount, UserBehavior}
import com.zitd.hotitemAnalysis.process.TopNHotItems
import com.zitd.hotitemAnalysis.windows.WindowResult
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object HotItems {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.readTextFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/HotItemAnalysis/src/main/resources/UserBehavior.csv")

    /*val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val inputStream: DataStream[String] = env.addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )*/

    // 转换成样例类类型，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      }).assignAscendingTimestamps(_.timestamp * 1000L)

    // 进行开窗聚合转换
    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult())

    // 对统计聚合结果按照窗口分组，排序输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process( new TopNHotItems(5) )

    resultStream.print()

    env.execute("hot items job")

  }

}
