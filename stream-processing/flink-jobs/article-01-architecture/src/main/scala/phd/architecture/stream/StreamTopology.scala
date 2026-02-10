package phd.architecture.stream

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import phd.architecture.model.{Event, SpatialPartition, WindowResult}
import phd.architecture.model.TypeInfos._


object StreamTopology {

  /**
   * Applies sliding event-time window:
   *
   * Ω(W, p)
   */
  def applySlidingWindow(
    stream: KeyedStream[Event, SpatialPartition],
    windowSizeSeconds: Int,
    slideSeconds: Int,
    aggregation: ProcessWindowFunction[
      Event,
      WindowResult,
      SpatialPartition,
      TimeWindow
    ]
  ): DataStream[WindowResult] = {

    stream
      .window(
        SlidingEventTimeWindows.of(
          Time.seconds(windowSizeSeconds),
          Time.seconds(slideSeconds)
        )
      )
      .process(aggregation)
  }

  def sinkResults(results: DataStream[WindowResult]): Unit =
    results.print()
}
