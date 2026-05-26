package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import phd.adaptivecontrol.model.GeoEvent


/**
  * WindowProcessor
  *
  * Centralized event-time window management layer.
  *
  * Responsibilities:
  *   - window creation
  *   - adaptive window sizing integration
  *   - temporal batching
  *
  * IMPORTANT:
  * Initial implementation uses static tumbling windows.
  *
  * Adaptive window resizing will later be integrated
  * through:
  *   - StreamProfiler
  *   - WindowController
  *   - RuntimeFeedback
  */
object WindowProcessor {

  // ============================================================
  // Tumbling Event-Time Window
  // ============================================================
  def applyWindow(
    stream: DataStream[GeoEvent]
  ): WindowedStream[
    GeoEvent,
    String,
    TimeWindow
  ] = {

    val windowSizeMs =
      sys.env
        .getOrElse("WINDOW_SIZE_MS", "5000")
        .toLong

    println(
      s"[WindowProcessor] windowSizeMs=$windowSizeMs"
    )

    stream
      .keyBy(_.objectId)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSizeMs)))
  }
}
