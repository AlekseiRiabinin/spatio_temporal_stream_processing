package phd.adaptivecontrol.pipeline

import java.time.Duration

import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.adaptive.StreamProfiler


/**
 * WatermarkManager
 *
 * Centralized watermark construction using AdaptiveConfig.
 *
 * Supports:
 *   - fixed watermark delay
 *   - adaptive watermark delay (ML-driven)
 */
object WatermarkManager {

  // ============================================================
  // Build Watermark Strategy from AdaptiveConfig
  // ============================================================
  def build(config: AdaptiveConfig): WatermarkStrategy[GeoEvent] = {

    // ------------------------------------------------------------
    // Select watermark delay based on strategy
    // ------------------------------------------------------------
    val delayMs: Long =
      config.watermarkStrategy match {

        case "adaptive" =>
          // Use adaptive value (updated dynamically)
          config.adaptiveWatermarkDelayMs

        case "fixed" | _ =>
          // Default: use configured fixed watermark delay
          config.watermarkDelayMs
      }

    println(
      s"[WATERMARK MANAGER] action=build " +
      s"strategy=${config.watermarkStrategy} " +
      s"effectiveDelay=$delayMs"
    )

    // ------------------------------------------------------------
    // Build Flink watermark strategy
    // ------------------------------------------------------------
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
