package phd.adaptivecontrol.pipeline

import java.time.Duration

import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.config.AdaptiveConfig


/**
 * WatermarkManager
 *
 * Centralized watermark construction using experiment config.
 *
 * Fully decoupled from environment variables.
 */
object WatermarkManager {

  // ============================================================
  // Build Watermark Strategy from AdaptiveConfig
  // ============================================================
  def build(config: AdaptiveConfig): WatermarkStrategy[GeoEvent] = {

    val delayMs = config.watermarkDelayMs

    println(
      s"[WatermarkManager] action=build delayMs=$delayMs"
    )

    WatermarkStrategy
      .forBoundedOutOfOrderness[GeoEvent](Duration.ofMillis(delayMs))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[GeoEvent] {

          override def extractTimestamp(
            event: GeoEvent,
            recordTimestamp: Long
          ): Long = event.timestamp
        }
      )
  }
}
