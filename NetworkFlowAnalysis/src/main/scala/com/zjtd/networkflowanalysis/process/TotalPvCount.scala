package com.zjtd.networkflowanalysis.process

import com.zjtd.networkflowanalysis.bean.PvCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

// 自定义的合并各key统计结果的ProcessFunction
class TotalPvCount() extends KeyedProcessFunction[Long, PvCount, PvCount]{

  // 定义一个状态，用来保存当前已有的key的count值总计
  lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

  override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {

    val currentTotalCount: Long = totalCountState.value()
    totalCountState.update(currentTotalCount + value.count)

    // 注册一个定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd +1 )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
    // 定时器触发时，直接输出当前的totalcount
    out.collect(PvCount(ctx.getCurrentKey,totalCountState.value()))
    // 清空状态
    totalCountState.clear()

  }
}
