package phd.architecture.stream

import org.apache.flink.api.common.eventtime._
import java.time.Duration
import phd.architecture.model.Event
import phd.architecture.metrics.MetricsRegistry


object WatermarkStrategyFactory {

  def eventTimeWatermarks(maxOutOfOrdernessSeconds: Long): WatermarkStrategy[Event] = {

    new WatermarkStrategy[Event] {

      override def createTimestampAssigner(
        context: TimestampAssignerSupplier.Context
      ): TimestampAssigner[Event] =
        new TimestampAssigner[Event] {

          override def extractTimestamp(
            element: Event,
            recordTimestamp: Long
          ): Long = {

            val ts = element.eventTime

            // record event-time extraction 
            MetricsRegistry.recordWatermark(
              s"event-time id=${element.id} eventTs=$ts"
            )

            ts
          }
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

          maxTs = Math.max(maxTs, eventTimestamp)
          val watermark = maxTs - maxOutOfOrdernessMs

          // record watermark progression
          MetricsRegistry.recordWatermark(
            s"watermark eventTs=$eventTimestamp maxTs=$maxTs wm=$watermark"
          )
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {
          val watermark = maxTs - maxOutOfOrdernessMs
          output.emitWatermark(new Watermark(watermark))
        }
      }
    }
  }
}
