package phd.architecture.operators

import phd.architecture.model.{Event, SpatialPartition, WindowResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
import phd.architecture.metrics.MetricsRegistry


final class CountEventsWindowFunction
  extends ProcessWindowFunction[Event, WindowResult, SpatialPartition, TimeWindow] {

  /**
   * Ω_count(W, p) = |{e ∈ W(p,T)}|
   * Counts number of events in a window per spatial partition.
   */
  override def process(
    key: SpatialPartition,
    context: Context,
    elements: Iterable[Event],
    out: Collector[WindowResult]
  ): Unit = {

    val count = elements.size.toLong
    val start = context.window.getStart
    val end = context.window.getEnd
    val duration = end - start

    MetricsRegistry.recordWindow(
      s"window partition=${key.geohash} start=$start end=$end " +
      s"durationMs=$duration count=$count"
    )

    out.collect(
      WindowResult(
        partition = key,
        windowStart = start,
        windowEnd = end,
        value = count,
        processingTime = None
      )
    )
  }
}

object WindowAggregation {
  val countEvents = new CountEventsWindowFunction
}
