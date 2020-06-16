package com.zjtd.networkflowanalysis.custom

import com.zjtd.networkflowanalysis.bean.UserBehavior
import org.apache.flink.api.common.functions.MapFunction

import scala.util.Random

// 实现自定义的Mapper
class Mapper extends MapFunction[UserBehavior,(String,Long)]{
  override def map(value: UserBehavior): (String, Long) = {
    (Random.nextString(4),1L)
  }
}
