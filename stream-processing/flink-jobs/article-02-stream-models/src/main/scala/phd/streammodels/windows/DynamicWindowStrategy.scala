package phd.streammodels.windows

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.model.TypeInfos._


/**
  * Dynamic window strategy:
  * - Window size adapts based on event density
  * - High density → smaller windows
  * - Low density → larger windows
  *
  * This is a simplified version:
  *   windowSize = baseSize / (1 + densityFactor)
  *
  * In a real system, densityFactor would come from a model or statistics.
  */
class DynamicWindowStrategy[K : TypeInformation](
  baseWindowSeconds: Long,
  densityFactor: Double,
  keySelector: Event => K
) extends WindowStrategy[K] {

  override val name: String = "dynamic"

  override def applyWindow(
    stream: DataStream[Event]
  ): DataStream[WindowResult[K]] = {

    // Compute adaptive window size
    val adjustedSeconds =
      Math.max(1, (baseWindowSeconds / (1.0 + densityFactor)).toLong)

    val windowSize = Time.seconds(adjustedSeconds)

    stream
      .keyBy(keySelector)
      .window(TumblingEventTimeWindows.of(windowSize))
      .apply(new CountEventsWindowFunction[K])
  }
}
