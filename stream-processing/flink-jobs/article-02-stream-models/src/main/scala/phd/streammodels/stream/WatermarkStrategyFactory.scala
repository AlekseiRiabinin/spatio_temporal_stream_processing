package phd.streammodels.stream

import org.apache.flink.api.common.eventtime._
import org.apache.flink.metrics.Gauge
import phd.streammodels.model.{Event, StreamModelType}


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
          ): Long = {
            // Trust event timestamps directly.
            val now = System.currentTimeMillis()
            Math.min(element.eventTime, now)
          }
        }

      override def createWatermarkGenerator(
        context: WatermarkGeneratorSupplier.Context
      ): WatermarkGenerator[Event] = new WatermarkGenerator[Event] {

        private var maxTs: Long = Long.MinValue

        // Allow override in milliseconds
        private val maxOutOfOrdernessMs: Long = {
          val fromEnvMs = sys.env.get("WATERMARK_MAX_DELAY_MS").map(_.toLong)
          fromEnvMs.getOrElse(maxOutOfOrdernessSeconds * 1000)
        }

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
          maxTs = Math.max(maxTs, eventTimestamp)
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {

          val watermark = maxTs - maxOutOfOrdernessMs

          val windowSizeMs =
            if (maxOutOfOrdernessMs > 0) maxOutOfOrdernessMs else 1000L

          val nextWindowEnd = ((watermark / windowSizeMs) + 1) * windowSizeMs

          println(
            s"""
               |[WATERMARK]
               |  watermark     = $watermark
               |  maxTs         = $maxTs
               |  nextWindowEnd = $nextWindowEnd
               |  currentTime   = ${System.currentTimeMillis()}
               |  outOfOrderMs  = $maxOutOfOrdernessMs
               |""".stripMargin
          )

          lastLag = maxTs - watermark
          output.emitWatermark(new Watermark(watermark))
        }
      }
    }
  }


  def forModel(modelType: StreamModelType): WatermarkStrategy[Event] = {

    val maxOutOfOrdernessSeconds: Long =
      sys.env.getOrElse("WATERMARK_OUT_OF_ORDERNESS", "5").toLong

    modelType match {

      case StreamModelType.Dataflow =>
        eventTimeWatermarks(maxOutOfOrdernessSeconds)

      case StreamModelType.MicroBatch =>
        new WatermarkStrategy[Event] {
          override def createTimestampAssigner(
            context: TimestampAssignerSupplier.Context
          ): TimestampAssigner[Event] =
            (event: Event, _: Long) => event.eventTime

          override def createWatermarkGenerator(
            context: WatermarkGeneratorSupplier.Context
          ): WatermarkGenerator[Event] =
            new WatermarkGenerator[Event] {

              private val batchSizeMs =
                sys.env.getOrElse("MICROBATCH_WM_DELAY_MS", "5000").toLong

              override def onEvent(
                event: Event,
                eventTimestamp: Long,
                output: WatermarkOutput
              ): Unit = {}

              override def onPeriodicEmit(output: WatermarkOutput): Unit = {
                val now = System.currentTimeMillis()
                val wm = now - batchSizeMs
                output.emitWatermark(new Watermark(wm))
              }
            }
        }

      case StreamModelType.Actor =>
        WatermarkStrategy.noWatermarks()

      case StreamModelType.Log =>
        WatermarkStrategy
          .forMonotonousTimestamps[Event]()
          .withTimestampAssigner(
            new SerializableTimestampAssigner[Event] {
              override def extractTimestamp(
                element: Event,
                recordTimestamp: Long
              ): Long = System.currentTimeMillis()
            }
          )
    }
  }
}
