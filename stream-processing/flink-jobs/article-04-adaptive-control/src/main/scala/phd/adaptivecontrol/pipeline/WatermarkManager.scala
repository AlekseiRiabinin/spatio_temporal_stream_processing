package phd.adaptivecontrol.pipeline

import java.time.Duration

import org.apache.flink.api.common.eventtime.{
  Watermark,
  WatermarkGenerator,
  WatermarkGeneratorSupplier,
  WatermarkOutput,
  WatermarkStrategy,
  SerializableTimestampAssigner
}

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.config.{AdaptiveConfig, StrategyMode}
import phd.adaptivecontrol.adaptive.{AdaptiveRuntimeState, StreamProfiler}


/**
 * WatermarkManager
 *
 * Clean design:
 * - NO string-based switching
 * - NO duplicated watermark logic
 * - Single responsibility per strategy
 */
object WatermarkManager {

  // ============================================================
  // Build Watermark Strategy
  // ============================================================
  def build(config: AdaptiveConfig): WatermarkStrategy[GeoEvent] = {

    config.watermarkMode match {

      // ========================================================
      // Adaptive Watermarks (runtime + ML controlled)
      // ========================================================
      case StrategyMode.Adaptive =>

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
          .withTimestampAssigner(
            new SerializableTimestampAssigner[GeoEvent] {
              override def extractTimestamp(
                event: GeoEvent,
                recordTimestamp: Long
              ): Long = event.timestamp
            }
          )

      // ========================================================
      // Fixed Watermarks (deterministic bounded delay)
      // ========================================================
      case StrategyMode.Fixed =>
        createFixedWatermarkStrategy(config)

    }
  }

  // ============================================================
  // Fixed watermark strategy (isolated, no duplication)
  // ============================================================
  private def createFixedWatermarkStrategy(
    config: AdaptiveConfig
  ): WatermarkStrategy[GeoEvent] = {

    println(
      "[WATERMARK MANAGER] action=build " +
      s"strategy=fixed " +
      s"effectiveDelay=${config.watermarkDelayMs}"
    )

    WatermarkStrategy
      .forGenerator(
        new WatermarkGeneratorSupplier[GeoEvent] {

          override def createWatermarkGenerator(
            context: WatermarkGeneratorSupplier.Context
          ): WatermarkGenerator[GeoEvent] = {

            new WatermarkGenerator[GeoEvent] {

              private var maxTimestampSeen: Long = Long.MinValue

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

                val watermarkTs =
                  maxTimestampSeen - config.watermarkDelayMs

                StreamProfiler.updateWatermark(watermarkTs)

                output.emitWatermark(new Watermark(watermarkTs))
              }
            }
          }
        }
      )
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
