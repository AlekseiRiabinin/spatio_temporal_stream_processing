package phd.streammodels.engine

import org.apache.flink.streaming.api.scala.DataStream
import phd.streammodels.model.{Event, WindowResult, StreamModelType}
import phd.streammodels.window.WindowStrategy


// Generic in K: the key / partition type
trait StreamModel[K] {
  def modelType: StreamModelType

  def buildPipeline(
    source: DataStream[Event],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]]
}
