package com.zjtd.marketanalysis.bean

/**
  * 分渠道统计样例类输出结果
  */
case class MarketingViewCount(windowStart: Long, windowEnd: Long, channel: String, behavior: String, count: Long)
