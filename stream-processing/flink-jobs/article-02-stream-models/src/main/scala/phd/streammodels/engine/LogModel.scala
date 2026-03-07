package phd.streammodels.engine

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.stream.WatermarkStrategyFactory
import phd.streammodels.window.WindowStrategy


/**
  * Log stream model (Article 2):
  * - Ingestion-time semantics
  * - Watermarks assume monotonically increasing timestamps
  * - Timestamp = system ingestion time, not event.eventTime
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

    // Assign ingestion-time timestamps and monotonic watermarks
    val withWatermarks: DataStream[Event] =
      source.assignTimestampsAndWatermarks(
        WatermarkStrategyFactory.forModel(modelType)
      )

    // Delegate windowing to the strategy
    windowStrategy.applyWindow(withWatermarks)
  }
}
