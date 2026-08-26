package cityrover.pipeline

import org.apache.flink.api.common.eventtime.{
  WatermarkStrategy,
  SerializableTimestampAssigner
}
import cityrover.model.GeoEvent
import cityrover.util.ConfigLoader
import java.time.Duration


/**
  * Centralized watermark strategy for GeoEvent streams.
  *
  * Uses a fixed out-of-order delay from application.conf:
  *
  *   cityrover.pipeline.watermark-delay-ms
  *
  * This keeps watermark behavior deterministic and reproducible,
  * which is essential for latency research.
  */
object Watermarking {

  private val delayMs: Long =
    ConfigLoader.watermarkDelayMs

  /**
    * Build a Flink WatermarkStrategy for GeoEvent.
    *
    * @return WatermarkStrategy[GeoEvent]
    */
  def strategy: WatermarkStrategy[GeoEvent] = {

    WatermarkStrategy
      .forBoundedOutOfOrderness[GeoEvent](Duration.ofMillis(delayMs))
      .withTimestampAssigner(new SerializableTimestampAssigner[GeoEvent] {
        override def extractTimestamp(
          event: GeoEvent,
          recordTimestamp: Long
        ): Long = event.ts
      })
  }
}
