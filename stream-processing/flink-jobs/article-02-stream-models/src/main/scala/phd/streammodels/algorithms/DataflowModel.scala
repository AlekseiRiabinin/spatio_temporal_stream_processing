package phd.streammodels.algorithms

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.stream.WatermarkStrategyFactory
import phd.streammodels.windows.WindowStrategy


/**
  * Dataflow stream model (Article 2):
  * - Event-time semantics
  * - Uses WatermarkStrategyFactory.forModel(StreamModelType.Dataflow)
  * - Delegates windowing to a WindowStrategy[K]
  */
class DataflowModel[K : TypeInformation](
  env: StreamExecutionEnvironment
) extends StreamModel[K] {

  override val modelType: StreamModelType = StreamModelType.Dataflow

  override def buildPipeline(
    source: DataStream[Event],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    val withWatermarks: DataStream[Event] =
      source.assignTimestampsAndWatermarks(
        WatermarkStrategyFactory.forModel(modelType)
      )

    windowStrategy.applyWindow(withWatermarks)
  }
}
