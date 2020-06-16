package com.zitd.hotitemAnalysis.aggregate

import com.zitd.hotitemAnalysis.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

// 自定义预聚合函数，每来一个数据就count加1
class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
