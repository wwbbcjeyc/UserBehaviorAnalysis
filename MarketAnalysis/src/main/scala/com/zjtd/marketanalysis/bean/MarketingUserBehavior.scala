package com.zjtd.marketanalysis.bean

/**
  * 源数据的样例类
  * @param userId
  * @param behavior
  * @param channel
  * @param timestamp
  */
case class MarketingUserBehavior(userId: Long, behavior: String, channel: String, timestamp: Long)
