package phd.streammodels.windows

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import phd.streammodels.model.{Event, WindowResult}


class CountEventsWindowFunction[K]
  extends WindowFunction[
    Event,               // input type
    WindowResult[K],     // output type
    K,                   // key type
    TimeWindow           // window type
  ] {

  override def apply(
    key: K,
    window: TimeWindow,
    input: Iterable[Event],
    out: Collector[WindowResult[K]]
  ): Unit = {

    val count = input.size

    out.collect(
      WindowResult(
        partition = key,
        windowStart = window.getStart,
        windowEnd = window.getEnd,
        value = count,
        processingTime = Some(System.currentTimeMillis())
      )
    )
  }
}
