package phd.architecture.stream

import org.apache.flink.api.common.eventtime._
import phd.architecture.model.Event
import phd.architecture.metrics.Metrics


object WatermarkStrategyFactory {

  /**
    * Event-time watermark strategy with bounded out-of-orderness.
    * Emits Prometheus watermark-lag metrics without modifying the job graph.
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

          val lag = maxTs - watermark
          Metrics.watermarkLag.set(lag)

          output.emitWatermark(new Watermark(watermark))
        }
      }
    }
  }
}
