package com.zjtd.networkflowanalysis

import com.zjtd.networkflowanalysis.bean.{UserBehavior, UvCount}
import com.zjtd.networkflowanalysis.windows.UvCountResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object UniqueVisitor {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(4)

    val inputStream: DataStream[String] = env.readTextFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 进行开窗统计聚合
    val uvCountStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll( Time.hours(1) )    // 统计每小时的uv值
      //        .map( data => (Random.nextString(10), data.userId) )
      //        .keyBy(_._1)
      //        .timeWindow( Time.hours(1) )
      //        .aggregate( new UvCountAgg(), new UvCountResult() )
      .apply( new UvCountResult() )

    uvCountStream.print()

    env.execute("uv job")


  }

}
