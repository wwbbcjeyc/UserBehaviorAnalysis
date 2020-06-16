package com.zjtd.networkflowanalysis.windows

import com.zjtd.networkflowanalysis.bean.PageViewCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 自定义窗口函数
class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect( PageViewCount(key, window.getEnd, input.iterator.next()) )
  }
}
