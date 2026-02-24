package phd.streammodels.algorithms

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.windows.WindowStrategy


/**
  * Actor stream model (Article 2):
  * - Pure processing-time semantics
  * - No watermarks
  * - Events are processed immediately as they arrive
  */
class ActorModel[K : TypeInformation](
  env: StreamExecutionEnvironment
) extends StreamModel[K] {

  override val modelType: StreamModelType = StreamModelType.Actor

  override def buildPipeline(
    source: DataStream[Event],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    // Processing-time model → no watermarks, no timestamp extraction
    val processingTimeStream: DataStream[Event] = source

    // Delegate windowing to the strategy
    windowStrategy.applyWindow(processingTimeStream)
  }
}
