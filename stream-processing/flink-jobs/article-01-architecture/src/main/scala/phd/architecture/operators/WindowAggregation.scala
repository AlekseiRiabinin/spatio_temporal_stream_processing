package phd.architecture.operators

import phd.architecture.model.{Event, SpatialPartition, WindowResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector


final class CountEventsWindowFunction
  extends ProcessWindowFunction[Event, WindowResult, SpatialPartition, TimeWindow] {

  /**
   * Ω_count(W, p) = {e∈W ∣ π(e)=p}
   * Counts number of events in a window per spatial partition
   */
  override def process(
    key: SpatialPartition,
    context: Context,
    elements: Iterable[Event],
    out: Collector[WindowResult]
  ): Unit = {

    val count = elements.size.toLong

    out.collect(
      WindowResult(
        partition = key,
        windowStart = context.window.getStart,
        windowEnd = context.window.getEnd,
        value = count,
        processingTime = None
      )
    )
  }
}

object WindowAggregation {
  val countEvents = new CountEventsWindowFunction
}
