package com.zjtd.networkflowanalysis

import java.text.SimpleDateFormat

import com.zjtd.networkflowanalysis.aggreate.PageCountAgg
import com.zjtd.networkflowanalysis.bean.{ApacheLogEvent, PageViewCount}
import com.zjtd.networkflowanalysis.process.TopNHotPages
import com.zjtd.networkflowanalysis.windows.PageCountWindowResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object NetWorkFlow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream: DataStream[String] = env.readTextFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/apache.log")

    val dataStream: DataStream[ApacheLogEvent] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(" ")

        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    val aggStream: DataStream[PageViewCount] = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url) // 按照页面url做分组开窗聚合统计
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    val resultStream: DataStream[String] = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    dataStream.print("data")
    aggStream.print("agg")
    resultStream.print("result")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")

    env.execute("hot page job")

  }

}
