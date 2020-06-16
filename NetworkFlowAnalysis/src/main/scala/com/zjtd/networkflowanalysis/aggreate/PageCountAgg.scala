package com.zjtd.networkflowanalysis.aggreate

import com.zjtd.networkflowanalysis.bean.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction


// 自定义预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
