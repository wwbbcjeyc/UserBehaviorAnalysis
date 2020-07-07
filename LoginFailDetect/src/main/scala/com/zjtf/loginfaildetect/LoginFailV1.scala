package com.zjtf.loginfaildetect

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

//实现自定义processFunction --改进版本
case class LoginFailWaring(maxFailTime: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义List状态，用来保存2秒内所有登陆失败事件
  lazy val loginFaillistState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("save-loginfail", classOf[LoginEvent]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 首先按照type做筛选，如果success直接清空，如果fail再做处理
    if ( value.eventType == "fail" ){
      // 如果已经有登录失败的数据，那么就判断是否在两秒内
      val iter: util.Iterator[LoginEvent] = loginFaillistState.get().iterator()
      if ( iter.hasNext ){
        val firstFail: LoginEvent = iter.next()
        // 如果两次登录失败时间间隔小于2秒，输出报警
        if ( value.eventTime < firstFail.eventTime + 2 ){
          out.collect( Warning( value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds." ) )
        }
        // 把最近一次的登录失败数据，更新写入state中
        val failList = new util.ArrayList[LoginEvent]()
        failList.add(value)
        loginFaillistState.update( failList )

      }else {
        // 如果state中没有登录失败的数据，那就直接添加进去
        loginFaillistState.add(value)
      }
    }else {

      loginFaillistState.clear()

    }



  }
}