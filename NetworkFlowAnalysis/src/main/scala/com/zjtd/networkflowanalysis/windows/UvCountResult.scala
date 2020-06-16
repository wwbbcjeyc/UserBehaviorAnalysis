package com.zjtd.networkflowanalysis.windows

import com.zjtd.networkflowanalysis.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UvCountResult extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 用一个集合来保存所有的userId，实现自动去重
    var idSet: Set[Long] = Set[Long]()

    for (ub <- input) {
      idSet += ub.userId
    }
    // 包装好样例类类型输出

    out.collect(UvCount(window.getEnd,idSet.size))

  }
}
