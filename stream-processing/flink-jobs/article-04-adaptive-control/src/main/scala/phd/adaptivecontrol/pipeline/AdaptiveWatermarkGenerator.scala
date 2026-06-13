package phd.adaptivecontrol.pipeline

import org.apache.flink.api.common.eventtime.{
  Watermark,
  WatermarkGenerator,
  WatermarkOutput
}

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.GeoEvent


class AdaptiveWatermarkGenerator(config: AdaptiveConfig)
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

    val delayMs =
      math.max(0L, config.adaptiveWatermarkDelayMs)

    val watermarkTs =
      maxTimestampSeen - delayMs

    output.emitWatermark(new Watermark(watermarkTs))

    println(
      "[ADAPTIVE WATERMARK] " +
      s"watermark=$watermarkTs " +
      s"delayMs=$delayMs " +
      s"maxTimestampSeen=$maxTimestampSeen"
    )
  }
}
