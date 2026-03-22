package phd.streammodels.engine

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.eventtime._
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.window.WindowStrategy


/**
  * Actor stream model (Article 2):
  * - Pure processing-time semantics
  * - For scientific comparison we add:
  *     [TIMESTAMP] logs
  *     [WATERMARK] logs
  */
class ActorModel[K : TypeInformation](
  env: StreamExecutionEnvironment
) extends StreamModel[K] {

  override val modelType: StreamModelType = StreamModelType.Actor

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

          // Emit processing-time timestamps + log them
          override def createTimestampAssigner(
            ctx: TimestampAssignerSupplier.Context
          ): TimestampAssigner[Event] =
            new TimestampAssigner[Event] {
              override def extractTimestamp(
                event: Event,
                recordTimestamp: Long
              ): Long = {
                val ts = System.currentTimeMillis()
                println(s"[TIMESTAMP] eventTime=$ts processingTime=$ts lag=0")
                ts
              }
            }

          // Emit synthetic processing-time watermarks + log them
          override def createWatermarkGenerator(
            ctx: WatermarkGeneratorSupplier.Context
          ): WatermarkGenerator[Event] =
            new WatermarkGenerator[Event] {

              override def onEvent(
                event: Event,
                eventTimestamp: Long,
                output: WatermarkOutput
              ): Unit = {
                // No per-event update needed
              }

              override def onPeriodicEmit(output: WatermarkOutput): Unit = {
                val now = System.currentTimeMillis()
                println(
                  s"""
                     |[WATERMARK]
                     |  watermark     = $now
                     |  currentTime   = $now
                     |  synthetic     = true
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
