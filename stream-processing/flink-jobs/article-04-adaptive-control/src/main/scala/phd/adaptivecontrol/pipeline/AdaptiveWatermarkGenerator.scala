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

    maxTimestampSeen =
      math.max(maxTimestampSeen, eventTimestamp)
  }

  override def onPeriodicEmit(output: WatermarkOutput): Unit = {

    if (maxTimestampSeen == Long.MinValue)
      return

    // --------------------------------------------------------
    // runtime delay retrieval
    // --------------------------------------------------------
    val delayMs =
      math.max(0L, AdaptiveRuntimeState.watermarkDelayMs)

    val safeDelayMs =
      if (delayMs <= 0L) 3000L else delayMs   // fallback safety

    val watermarkTs =
      maxTimestampSeen - safeDelayMs

    // --------------------------------------------------------
    // expose watermark to profiler
    // --------------------------------------------------------
    StreamProfiler.updateWatermark(watermarkTs)

    // Emit watermark into Flink
    output.emitWatermark(new Watermark(watermarkTs))

    // Logging
    println(
      "[ADAPTIVE WATERMARK] " +
      s"watermark=$watermarkTs " +
      s"delayMs=$safeDelayMs " +
      s"maxTimestampSeen=$maxTimestampSeen"
    )
  }
}
