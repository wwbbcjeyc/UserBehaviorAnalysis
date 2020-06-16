package com.zjtd.networkflowanalysis.aggreate

import org.apache.flink.api.common.functions.AggregateFunction

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
