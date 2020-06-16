package com.zjtd.networkflowanalysis.windows


import com.zjtd.networkflowanalysis.bean.PvCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 自定义窗口函数
class PvCountWindowResult() extends WindowFunction[Long,PvCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input:Iterable[Long], out: Collector[PvCount]): Unit = {
      out.collect(PvCount(window.getEnd,input.iterator.next()))
  }
}
