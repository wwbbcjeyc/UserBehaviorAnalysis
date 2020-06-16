package com.zjtd.networkflowanalysis.process

import java.sql.Timestamp
import java.util
import java.util.Map

import com.zjtd.networkflowanalysis.bean.PageViewCount
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 自定义KeyedProcessFunction，实现count结果的排序
case class TopNHotPages(topSize: Int) extends KeyedProcessFunction[Long,PageViewCount,String]{

  // 定义列表状态，用来保存当前窗口的所有page的count值
  //  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageview-count", classOf[PageViewCount]))

  // 改进：定义MapState，用来保存当前窗口所有page的count值，有更新操作时直接put
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageview-count", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //    pageViewCountListState.add(value)
        pageViewCountMapState.put(value.url,value.count)
        ctx.timerService().registerEventTimeTimer(value.windowEnd +1 )
    // 定义1分钟之后的定时器，用于清除状态
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 判断时间戳，如果是1分钟后的定时器，直接清空状态
    if(timestamp == ctx.getCurrentKey + 60 * 1000L){
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    //    val iter = pageViewCountListState.get().iterator()
    //    while(iter.hasNext)
    //      allPageViewCounts += iter.next()

    val iter: util.Iterator[Map.Entry[String, Long]] = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += PageViewCount(entry.getKey, timestamp - 1, entry.getValue)
    }

    //    pageViewCountListState.clear()

    // 将所有count值排序取前 N 个
    val sortedPageViewCounts: ListBuffer[PageViewCount] = allPageViewCounts.sortWith(_.count > _.count).take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("==================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    for(i <- sortedPageViewCounts.indices){
      val currentViewCount: PageViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append(" 页面url=").append(currentViewCount.url)
        .append(" 访问量=").append(currentViewCount.count)
        .append("\n")
    }
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}
