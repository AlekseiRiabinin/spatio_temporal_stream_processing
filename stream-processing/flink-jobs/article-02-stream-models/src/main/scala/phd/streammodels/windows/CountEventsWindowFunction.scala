package phd.streammodels.windows

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction

import org.apache.flink.util.Collector
import phd.streammodels.model.{Event, WindowResult}


class CountEventsWindowFunction[K]
  extends ProcessWindowFunction[
    Event,               // input type
    WindowResult[K],     // output type
    K,                   // key type
    TimeWindow           // window type
  ] {

  override def process(
    key: K,
    context: Context,
    elements: Iterable[Event],
    out: Collector[WindowResult[K]]
  ): Unit = {

    val start = context.window.getStart
    val end   = context.window.getEnd
    val count = elements.size
    val wm    = context.currentWatermark

    println(
      s"""
         |[WINDOW RESULT]
         |  key              = $key
         |  elements         = $count
         |  windowStart      = $start
         |  windowEnd        = $end
         |  currentWatermark = $wm
         |""".stripMargin
    )

    out.collect(
      WindowResult(
        partition = key,
        windowStart = start,
        windowEnd = end,
        value = count,
        processingTime = Some(System.currentTimeMillis())
      )
    )
  }
}
