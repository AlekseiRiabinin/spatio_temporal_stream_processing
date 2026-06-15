package phd.adaptivecontrol.pipeline

import org.apache.flink.api.common.eventtime.{
  Watermark,
  WatermarkGenerator,
  WatermarkOutput
}

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.config.{AdaptiveConfig, StrategyMode}
import phd.adaptivecontrol.adaptive.{
  AdaptiveRuntimeState,
  StreamProfiler
}


class AdaptiveWatermarkGenerator(
  config: AdaptiveConfig
) extends WatermarkGenerator[GeoEvent] {

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
    // Watermark delay selection
    // --------------------------------------------------------
    val delayMs =
      config.watermarkMode match {

        case StrategyMode.Adaptive =>
          math.max(
            0L,
            AdaptiveRuntimeState.watermarkDelayMs
          )

        case StrategyMode.Fixed =>
          math.max(
            0L,
            config.watermarkDelayMs
          )
      }

    val watermarkTs =
      maxTimestampSeen - delayMs

    // --------------------------------------------------------
    // Expose watermark to profiler
    // --------------------------------------------------------
    StreamProfiler.updateWatermark(watermarkTs)

    // --------------------------------------------------------
    // Emit watermark into Flink
    // --------------------------------------------------------
    output.emitWatermark(
      new Watermark(watermarkTs)
    )

    println(
      "[ADAPTIVE WATERMARK] " +
      s"watermark=$watermarkTs " +
      s"delayMs=$delayMs " +
      s"maxTimestampSeen=$maxTimestampSeen"
    )
  }
}
