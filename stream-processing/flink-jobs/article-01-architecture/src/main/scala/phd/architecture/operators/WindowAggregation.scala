package phd.architecture.operators

import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
import phd.architecture.model.{Event, SpatialPartition, WindowResult}
import phd.architecture.metrics.Metrics


/**
 * Ω_count(W, p) = |{e ∈ W(p,T)}|
 * Counts number of events in a window per spatial partition.
 * Emits Prometheus metrics without modifying the job graph.
 */
final class CountEventsWindowFunction
  extends ProcessWindowFunction[Event, WindowResult, SpatialPartition, TimeWindow] {

  override def process(
    key: SpatialPartition,
    context: Context,
    elements: Iterable[Event],
    out: Collector[WindowResult]
  ): Unit = {

    val count = elements.size.toLong
    val start = context.window.getStart
    val end = context.window.getEnd

    val now = context.currentProcessingTime

    Metrics.windowLatency.observe((now - end) / 1000.0)  // seconds
    Metrics.processingLatency.set(now - end)             // ms

    out.collect(
      WindowResult(
        partition = key,
        windowStart = start,
        windowEnd = end,
        value = count,
        processingTime = Some(now)
      )
    )
  }
}

object WindowAggregation {
  val countEvents = new CountEventsWindowFunction
}
