package phd.adaptivecontrol.pipeline

import java.time.Duration

import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkGeneratorSupplier,
  WatermarkStrategy
}

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.adaptive.AdaptiveRuntimeState


/**
 * WatermarkManager
 *
 * Supports:
 *   - fixed watermark strategy
 *   - adaptive watermark strategy
 */
object WatermarkManager {

  // ============================================================
  // Build Watermark Strategy
  // ============================================================
  def build(config: AdaptiveConfig): WatermarkStrategy[GeoEvent] = {

    config.watermarkStrategy match {

      // ========================================================
      // Adaptive Watermarks
      // ========================================================
      case "adaptive" =>

        println(
          "[WATERMARK MANAGER] action=build " +
          s"strategy=adaptive " +
          s"effectiveDelay=${AdaptiveRuntimeState.watermarkDelayMs}"
        )

        WatermarkStrategy
          .forGenerator(
            (_: WatermarkGeneratorSupplier.Context) =>
              new AdaptiveWatermarkGenerator
          )

      // ========================================================
      // Fixed Watermarks
      // ========================================================
      case "fixed" | _ =>

        println(
          "[WATERMARK MANAGER] action=build " +
          s"strategy=fixed " +
          s"effectiveDelay=${config.watermarkDelayMs}"
        )

        WatermarkStrategy
          .forBoundedOutOfOrderness[GeoEvent](
            Duration.ofMillis(config.watermarkDelayMs)
          )
          .withTimestampAssigner(
            new SerializableTimestampAssigner[GeoEvent] {
              override def extractTimestamp(
                event: GeoEvent,
                recordTimestamp: Long
              ): Long =
                event.timestamp
            }
          )
    }
  }
}
