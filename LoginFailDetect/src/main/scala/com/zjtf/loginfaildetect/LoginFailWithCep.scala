package com.zjtf.loginfaildetect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")

    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 2. 构造一个模式pattern
    Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail") //第一次登陆失败
      .next("secondFail").where(_.eventType=="fail") // 紧跟着第二次登录失败事
      .within(Time.seconds(2))

    // 循环模式定义示例
    val loginFailPattern2 = Pattern
      .begin[LoginEvent]("fails").times(2).where(_.eventType == "fail").consecutive()
      .within(Time.seconds(10))

    // 3. 将pattern应用到dataStream上，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern2)

    // 4. 检出符合规则匹配的复杂事件，转换成输出结果
    val loginFailWarningStream: DataStream[Warning] = patternStream.select(new LoginFailDetect2())

    // 5. 打印输出报警信息
    loginFailWarningStream.print("warning")

    env.execute("login fail with cep job")






  }

  // 实现自定义的PatternSelectFunction
  class LoginFailDetect2() extends PatternSelectFunction[LoginEvent, Warning]{

    override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
      val firstFailEvent: LoginEvent = map.get("firstFail").iterator().next()
      val secondFailEvent: LoginEvent = map.get("secondFail").iterator().next()
      Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail")
    }
  }

  class LoginFailDetect2() extends PatternSelectFunction[LoginEvent, Warning]{
    override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
      val firstFailEvent = pattern.get("fails").get(0)
      val secondFailEvent = pattern.get("fails").get(1)
      Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail")
    }}

}
