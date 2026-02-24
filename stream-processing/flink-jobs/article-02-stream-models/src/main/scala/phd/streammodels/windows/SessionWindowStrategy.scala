package phd.streammodels.windows

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import phd.streammodels.model.{Event, WindowResult}


/**
  * Session window strategy:
  * - Groups events by key K
  * - Creates a new session when the gap exceeds sessionGapSeconds
  * - Uses event-time semantics (or whatever the model provides)
  */
class SessionWindowStrategy[K : TypeInformation](
  sessionGapSeconds: Long,
  keySelector: Event => K
) extends WindowStrategy[K] {

  override def applyWindow(
    stream: DataStream[Event]
  ): DataStream[WindowResult[K]] = {

    stream
      .keyBy(keySelector)
      .window(EventTimeSessionWindows.withGap(Time.seconds(sessionGapSeconds)))
      .apply(new CountEventsWindowFunction[K])
  }
}
