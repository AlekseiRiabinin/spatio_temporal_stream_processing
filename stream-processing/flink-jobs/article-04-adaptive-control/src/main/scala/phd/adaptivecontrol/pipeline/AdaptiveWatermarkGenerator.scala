package phd.adaptivecontrol.pipeline

import org.apache.flink.api.common.eventtime.{
  Watermark,
  WatermarkGenerator,
  WatermarkOutput
}

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.adaptive.AdaptiveRuntimeState
import phd.adaptivecontrol.adaptive.StreamProfiler


class AdaptiveWatermarkGenerator
  extends WatermarkGenerator[GeoEvent] {

  private var maxTimestampSeen: Long =
    Long.MinValue

  override def onEvent(
    event: GeoEvent,
    eventTimestamp: Long,
    output: WatermarkOutput
  ): Unit = {

    // Track event-time progress
    maxTimestampSeen =
      math.max(maxTimestampSeen, eventTimestamp)
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {

    if (maxTimestampSeen == Long.MinValue)
      return

    // Adaptive delay from runtime state
    val delayMs =
      math.max(0L, AdaptiveRuntimeState.watermarkDelayMs)

    // Compute watermark (event-time - delay)
    val watermarkTs =
      maxTimestampSeen - delayMs

    StreamProfiler.updateWatermark(watermarkTs)

    // Emit watermark into Flink
    output.emitWatermark(new Watermark(watermarkTs))

    // Logging
    println(
      "[ADAPTIVE WATERMARK] " +
      s"watermark=$watermarkTs " +
      s"delayMs=$delayMs " +
      s"maxTimestampSeen=$maxTimestampSeen"
    )
  }
}
