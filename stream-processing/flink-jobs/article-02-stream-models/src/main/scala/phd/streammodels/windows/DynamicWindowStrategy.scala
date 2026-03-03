package phd.streammodels.windows

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.model.TypeInfos._


class DynamicWindowStrategy[K : TypeInformation](
  baseWindowSeconds: Long,
  densityFactor: Double,
  keySelector: Event => K
) extends WindowStrategy[K] {

  override val name: String = "dynamic"

  override def applyWindow(
    stream: DataStream[Event]
  ): DataStream[WindowResult[K]] = {

    val adjustedSeconds =
      Math.max(1, (baseWindowSeconds / (1.0 + densityFactor)).toLong)

    println(
      s"""
         |[WINDOW] Applying DynamicWindowStrategy
         |    baseWindowSeconds = $baseWindowSeconds
         |    densityFactor     = $densityFactor
         |    adjustedSeconds   = $adjustedSeconds
         |""".stripMargin
    )

    stream
      .keyBy(keySelector)
      .window(TumblingEventTimeWindows.of(Time.seconds(adjustedSeconds)))
      .apply { (
        key: K,
        window: TimeWindow,
        input: Iterable[Event],
        out: Collector[WindowResult[K]]
      ) =>

        val count = input.size

        println(
          s"""
             |[WINDOW RESULT]
             |  key         = $key
             |  windowStart = ${window.getStart}
             |  windowEnd   = ${window.getEnd}
             |  count       = $count
             |  timestamp   = ${System.currentTimeMillis()}
             |""".stripMargin
        )

        out.collect(
          WindowResult(
            partition = key,
            windowStart = window.getStart,
            windowEnd = window.getEnd,
            value = count,
            processingTime = Some(System.currentTimeMillis())
          )
        )
      }
  }
}
