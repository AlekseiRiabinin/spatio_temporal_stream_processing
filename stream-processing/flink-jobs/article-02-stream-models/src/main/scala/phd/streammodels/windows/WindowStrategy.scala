package phd.streammodels.windows

import org.apache.flink.streaming.api.scala.DataStream
import phd.streammodels.model.{Event, WindowResult} 


trait WindowStrategy[K] {
  def name: String
  def applyWindow(stream: DataStream[Event]): DataStream[WindowResult[K]]
}
