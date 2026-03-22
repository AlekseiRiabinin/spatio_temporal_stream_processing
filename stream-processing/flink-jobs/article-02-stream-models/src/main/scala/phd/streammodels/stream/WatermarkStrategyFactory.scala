package phd.streammodels.stream

import org.apache.flink.api.common.eventtime._
import org.apache.flink.metrics.Gauge
import phd.streammodels.model.{Event, StreamModelType}


object WatermarkStrategyFactory {

  /**
    * Event-time watermark strategy with bounded out-of-orderness.
    * Emits watermark-lag metrics and debug logs.
    */
  def eventTimeWatermarks(maxOutOfOrdernessSeconds: Long): WatermarkStrategy[Event] = {

    new WatermarkStrategy[Event] {

      override def createTimestampAssigner(
        context: TimestampAssignerSupplier.Context
      ): TimestampAssigner[Event] =
        new TimestampAssigner[Event] {

          private val debug =
            sys.env.getOrElse("DEBUG_TIMESTAMPS", "true").toBoolean

          override def extractTimestamp(
            element: Event,
            recordTimestamp: Long
          ): Long = {

            val ts = element.eventTime

            if (debug) {
              val now = System.currentTimeMillis()
              println(
                s"[TIMESTAMP] eventTime=$ts processingTime=$now lag=${now - ts}"
              )
            }

            ts
          }
        }

      override def createWatermarkGenerator(
        context: WatermarkGeneratorSupplier.Context
      ): WatermarkGenerator[Event] = new WatermarkGenerator[Event] {

        private var maxTs: Long = Long.MinValue

        private val maxOutOfOrdernessMs: Long =
          sys.env
            .get("WATERMARK_MAX_DELAY_MS")
            .map(_.toLong)
            .getOrElse(maxOutOfOrdernessSeconds * 1000)

        @transient private var lastLag: Long = 0L

        private val debug =
          sys.env.getOrElse("DEBUG_WATERMARKS", "true").toBoolean

        // Register metric for Prometheus
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

          if (eventTimestamp > maxTs)
            maxTs = eventTimestamp
        }

        override def onPeriodicEmit(output: WatermarkOutput): Unit = {

          if (maxTs == Long.MinValue)
            return

          val currentTime = System.currentTimeMillis()

          val watermark = maxTs - maxOutOfOrdernessMs

          val windowSizeMs =
            if (maxOutOfOrdernessMs > 0) maxOutOfOrdernessMs else 1000L

          val nextWindowEnd =
            ((watermark / windowSizeMs) + 1) * windowSizeMs

          if (debug) {
            println(
              s"""
                 |[WATERMARK]
                 |  watermark     = $watermark
                 |  maxTs         = $maxTs
                 |  nextWindowEnd = $nextWindowEnd
                 |  currentTime   = $currentTime
                 |  outOfOrderMs  = $maxOutOfOrdernessMs
                 |""".stripMargin
            )
          }

          lastLag = currentTime - watermark

          output.emitWatermark(new Watermark(watermark))
        }
      }
    }
  }

  def forModel(modelType: StreamModelType): WatermarkStrategy[Event] = {

    val maxOutOfOrdernessSeconds =
      sys.env.getOrElse("WATERMARK_OUT_OF_ORDERNESS", "5").toLong

    modelType match {

      case StreamModelType.Dataflow =>
        eventTimeWatermarks(maxOutOfOrdernessSeconds)

      case StreamModelType.MicroBatch =>
        new WatermarkStrategy[Event] {

          override def createTimestampAssigner(
            context: TimestampAssignerSupplier.Context
          ): TimestampAssigner[Event] =
            new TimestampAssigner[Event] {
              override def extractTimestamp(event: Event, recordTimestamp: Long): Long = {
                val ts = event.eventTime
                val now = System.currentTimeMillis()
                println(s"[TIMESTAMP] eventTime=$ts processingTime=$now lag=${now - ts}")
                ts
              }
            }

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

                println(
                  s"""
                    |[WATERMARK]
                    |  watermark     = $wm
                    |  currentTime   = $now
                    |  batchSizeMs   = $batchSizeMs
                    |""".stripMargin
                )

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
