package phd.adaptivecontrol.pipeline

import java.time.Duration

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner

import phd.adaptivecontrol.model.GeoEvent


/**
  * WatermarkManager
  *
  * Centralized manager for Flink watermark strategies.
  *
  * Responsibilities:
  *   - event-time extraction
  *   - watermark delay configuration
  *   - adaptive watermark support
  *
  * IMPORTANT:
  * Initial implementation uses static bounded
  * out-of-orderness watermarks.
  *
  * Adaptive watermark logic will later be integrated
  * through RuntimeFeedback + WatermarkController.
  */
object WatermarkManager {

  // ============================================================
  // Static Watermark Strategy
  // ============================================================
  def buildStrategy(watermarkDelayMs: Long): WatermarkStrategy[GeoEvent] = {

    println(
      s"[WatermarkManager] delayMs=$watermarkDelayMs"
    )

    WatermarkStrategy
      .forBoundedOutOfOrderness[GeoEvent](Duration.ofMillis(watermarkDelayMs))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[GeoEvent] {

          override def extractTimestamp(
            event: GeoEvent,
            recordTimestamp: Long
          ): Long = {

            event.timestamp
          }
        }
      )
  }

  // ============================================================
  // Environment-Based Configuration
  // ============================================================
  def fromEnv(): WatermarkStrategy[GeoEvent] = {

    val delayMs =
      sys.env
        .getOrElse("WATERMARK_DELAY_MS", "3000")
        .toLong

    buildStrategy(delayMs)
  }
}
