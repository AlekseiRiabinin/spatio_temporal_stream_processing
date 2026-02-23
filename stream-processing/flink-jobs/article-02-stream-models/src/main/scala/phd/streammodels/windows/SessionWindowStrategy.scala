package phd.streammodels.windows

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.operators.WindowAggregation


class SessionWindowStrategy extends WindowStrategy[String] {

  override val name: String = "session"

  override def applyWindow(stream: DataStream[Event]): DataStream[WindowResult[String]] = {
    stream
      .keyBy(_.id) // key type = String
      .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
      .process(WindowAggregation.countEvents[String])
  }
}
