package phd.adaptivecontrol.pipeline

import java.time.Duration

import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}

import phd.adaptivecontrol.model.GeoEvent


/**
 * WatermarkManager
 *
 * Centralized watermark management layer for:
 *   - event-time extraction
 *   - out-of-order handling
 *   - adaptive watermark integration
 *
 * IMPORTANT:
 * Initial implementation uses static bounded
 * out-of-orderness watermarks.
 *
 * Future adaptive logic will integrate:
 *   - StreamProfiler
 *   - AdaptiveController
 *   - RuntimeFeedback
 */
object WatermarkManager {

  // ------------------------------------------------------------
  // Default Configuration
  // ------------------------------------------------------------
  private val DefaultDelayMs = 3000L

  // ------------------------------------------------------------
  // Build Static Watermark Strategy
  // ------------------------------------------------------------
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

            val ts = event.timestamp
            val now = System.currentTimeMillis()
            val lag = now - ts

            println(
              s"[WatermarkManager] " +
              s"eventTime=$ts " +
              s"processingTime=$now " +
              s"lagMs=$lag " +
              s"objectId=${event.objectId}"
            )

            ts
          }
        }
      )
  }

  // ------------------------------------------------------------
  // Environment-Based Configuration
  // ------------------------------------------------------------
  def fromEnv(): WatermarkStrategy[GeoEvent] = {

    val delayMs =
      sys.env
        .getOrElse("WATERMARK_DELAY_MS", DefaultDelayMs.toString)
        .toLong

    buildStrategy(delayMs)
  }
}
