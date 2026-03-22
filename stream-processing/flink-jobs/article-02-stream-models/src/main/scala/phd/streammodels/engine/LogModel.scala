package phd.streammodels.engine

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.eventtime._
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.window.WindowStrategy


/**
  * Log stream model (Article 2):
  * - Ingestion-time semantics
  * - Watermarks assume monotonically increasing timestamps
  * - Timestamp = system ingestion time, not event.eventTime
  * - For scientific comparison, we add:
  *     [TIMESTAMP] logs
  *     [WATERMARK] logs
  */
class LogModel[K : TypeInformation](
  env: StreamExecutionEnvironment
) extends StreamModel[K] {

  override val modelType: StreamModelType = StreamModelType.Log

  override def buildPipeline(
    source: DataStream[Event],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    println(
      s"""
         |[MODEL] Building pipeline in ${getClass.getSimpleName}
         |    modelType      = $modelType
         |    windowStrategy = ${windowStrategy.getClass.getSimpleName}
         |""".stripMargin
    )

    val withWatermarks: DataStream[Event] =
      source.assignTimestampsAndWatermarks(
        new WatermarkStrategy[Event] {

          override def createTimestampAssigner(
            ctx: TimestampAssignerSupplier.Context
          ): TimestampAssigner[Event] =
            new TimestampAssigner[Event] {
              override def extractTimestamp(
                event: Event,
                recordTimestamp: Long
              ): Long = {
                val ts = System.currentTimeMillis()
                val now = ts
                println(s"[TIMESTAMP] eventTime=$ts processingTime=$now lag=0")
                ts
              }
            }

          override def createWatermarkGenerator(
            ctx: WatermarkGeneratorSupplier.Context
          ): WatermarkGenerator[Event] =
            new WatermarkGenerator[Event] {

              override def onEvent(
                event: Event,
                eventTimestamp: Long,
                output: WatermarkOutput
              ): Unit = {
                // No per-event update needed for monotonic timestamps
              }

              override def onPeriodicEmit(output: WatermarkOutput): Unit = {
                val now = System.currentTimeMillis()
                println(
                  s"""
                     |[WATERMARK]
                     |  watermark     = $now
                     |  currentTime   = $now
                     |  monotonic     = true
                     |""".stripMargin
                )
                output.emitWatermark(new Watermark(now))
              }
            }
        }
      )

    windowStrategy.applyWindow(withWatermarks)
  }
}
