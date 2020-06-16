package com.zjtd.networkflowanalysis

import com.zjtd.networkflowanalysis.aggreate.PvCountAgg
import com.zjtd.networkflowanalysis.bean.{PvCount, UserBehavior}
import com.zjtd.networkflowanalysis.custom.Mapper
import com.zjtd.networkflowanalysis.process.TotalPvCount
import com.zjtd.networkflowanalysis.windows.PvCountWindowResult
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(4)

    val inputStream: DataStream[String] = env.readTextFile("/Users/wangwenbo/IdeaProjects/UserBehaviorAnalysis/NetworkFlowAnalysis/src/main/resources/UserBehavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
    //dataStream.print()


    // 进行开窗统计聚合
    val pvCountStream: DataStream[PvCount] = dataStream
      .filter(_.behavior == "pv")
      //  .map(data => ("pv",1L)) // map成二元组，用一个哑key来作为分组的key
      .map(new Mapper())
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PvCountAgg(), new PvCountWindowResult())


    // 把每个key对应的pv count值合并
    val totalPvCountStream: DataStream[PvCount] = pvCountStream
      .keyBy(_.windowEnd)
      .process( new TotalPvCount() )

    totalPvCountStream.print()

    env.execute("pv job")
  }

}
