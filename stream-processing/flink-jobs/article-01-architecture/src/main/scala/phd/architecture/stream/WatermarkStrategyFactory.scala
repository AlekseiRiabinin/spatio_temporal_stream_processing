package phd.architecture.stream

import org.apache.flink.api.common.eventtime._
import org.apache.flink.metrics.Gauge
import phd.architecture.model.Event


object WatermarkStrategyFactory {

  /**
    * Event-time watermark strategy with bounded out-of-orderness.
    * Emits Flink watermark-lag metrics (Prometheus-compatible).
    */
  def eventTimeWatermarks(maxOutOfOrdernessSeconds: Long): WatermarkStrategy[Event] = {

    new WatermarkStrategy[Event] {

      override def createTimestampAssigner(
        context: TimestampAssignerSupplier.Context
      ): TimestampAssigner[Event] =
        new TimestampAssigner[Event] {
          override def extractTimestamp(
            element: Event,
            recordTimestamp: Long
          ): Long = element.eventTime
        }

      override def createWatermarkGenerator(
        context: WatermarkGeneratorSupplier.Context
      ): WatermarkGenerator[Event] = new WatermarkGenerator[Event] {

        private var maxTs: Long = Long.MinValue
        private val maxOutOfOrdernessMs = maxOutOfOrdernessSeconds * 1000

        // Metric: watermark lag (ms)
        @transient private var lastLag: Long = 0L

        // Register gauge
        context
          .getMetricGroup
          .gauge[Long, Gauge[Long]](
            "watermarkLag",
            new Gauge[Long] {
              override def getValue: Long = lastLag
            }
          )

        override def onEvent(
          event: Event,
          eventTimestamp: Long,
          output: WatermarkOutput
        ): Unit = {
          // Track max observed timestamp
          maxTs = Math.max(maxTs, eventTimestamp)
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {

          // Compute watermark
          val watermark = maxTs - maxOutOfOrdernessMs

          // Lag = max event time - watermark
          lastLag = maxTs - watermark

          output.emitWatermark(new Watermark(watermark))
        }
      }
    }
  }
}
