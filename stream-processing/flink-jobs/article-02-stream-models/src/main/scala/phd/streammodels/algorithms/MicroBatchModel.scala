package phd.streammodels.algorithms

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.stream.WatermarkStrategyFactory
import phd.streammodels.windows.WindowStrategy


/**
  * MicroBatch stream model (Article 2):
  * - Coarse-grained event-time semantics
  * - Watermarks advance in batch-sized jumps
  * - Simulates mini-batch processing inside Flink
  */
class MicroBatchModel[K : TypeInformation](
  env: StreamExecutionEnvironment
) extends StreamModel[K] {

  override val modelType: StreamModelType = StreamModelType.MicroBatch

  override def buildPipeline(
    source: DataStream[Event],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    // Assign coarse watermarks (batch-like)
    val withWatermarks: DataStream[Event] =
      source.assignTimestampsAndWatermarks(
        WatermarkStrategyFactory.forModel(modelType)
      )

    // Delegate windowing to the strategy
    windowStrategy.applyWindow(withWatermarks)
  }
}
