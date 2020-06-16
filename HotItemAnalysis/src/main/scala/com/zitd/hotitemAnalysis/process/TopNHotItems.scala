package com.zitd.hotitemAnalysis.process

import java.sql.Timestamp

import com.zitd.hotitemAnalysis.bean.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 自定义一个KeyedProcessFunction，对每个窗口的count统计值排序，并格式化成字符串输出
case class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

  // 定义一个列表状态，用来保存当前窗口的所有商品的count值
  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-liststate", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，就将它添加到ListState里
    itemViewCountListState.add(value)
    // 需要注册一个windowEnd+1的定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  // 当定时器触发时，当前窗口所有商品的统计数都到齐了，可以直接排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 遍历ListState的数据，全部放到一个ListBuffer中，方便排序
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for( itemViewCount <- itemViewCountListState.get() ){
      allItemViewCounts += itemViewCount
    }

    // 提前清空状态
    itemViewCountListState.clear()

    // 按照count大小排序并取前topSize
    val sortedItemViewCounts: ListBuffer[ItemViewCount] = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 将排序数据包装成可视化的String，便于打印输出
    val result: StringBuilder = new StringBuilder
    result.append("==================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 遍历排序结果数组，将每个ItemViewCount的商品ID和count值，以及排名输出
    for(i <- sortedItemViewCounts.indices){
      val currentViewCount: ItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append(" 商品ID=").append(currentViewCount.itemId)
        .append(" 点击量=").append(currentViewCount.count)
        .append("\n")
    }
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())


  }
}
