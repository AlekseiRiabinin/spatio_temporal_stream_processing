package phd.streammodels.windows

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.model.TypeInfos._


class MultiTriggerWindowStrategy[K : TypeInformation](
  windowSeconds: Long,
  keySelector: Event => K,
  countThreshold: Int,
  processingIntervalMs: Long
) extends WindowStrategy[K] {

  override val name: String = "multitrigger"

  override def applyWindow(
    stream: DataStream[Event]
  ): DataStream[WindowResult[K]] = {

    stream
      .keyBy(keySelector)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
      .trigger(
        new MultiTrigger[K](
          countThreshold = countThreshold,
          processingIntervalMs = processingIntervalMs
        )
      )
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

/**
  * Composite trigger: event-time, count-based, periodic processing-time.
  */
class MultiTrigger[K](
  countThreshold: Int,
  processingIntervalMs: Long
) extends Trigger[Event, TimeWindow] {

  override def onElement(
    element: Event,
    timestamp: Long,
    window: TimeWindow,
    ctx: Trigger.TriggerContext
  ): TriggerResult = {

    val countState = ctx.getPartitionedState(
      new ValueStateDescriptor[Long]("count", classOf[Long])
    )

    val newCount = Option(countState.value()).getOrElse(0L) + 1
    countState.update(newCount)

    if (newCount >= countThreshold) {
      countState.clear()
      return TriggerResult.FIRE
    }

    ctx.registerProcessingTimeTimer(
      ctx.getCurrentProcessingTime + processingIntervalMs
    )

    TriggerResult.CONTINUE
  }

  override def onProcessingTime(
    time: Long,
    window: TimeWindow,
    ctx: Trigger.TriggerContext
  ): TriggerResult = TriggerResult.FIRE

  override def onEventTime(
    time: Long,
    window: TimeWindow,
    ctx: Trigger.TriggerContext
  ): TriggerResult = {
    if (time == window.getEnd) TriggerResult.FIRE
    else TriggerResult.CONTINUE
  }

  override def clear(
    window: TimeWindow,
    ctx: Trigger.TriggerContext
  ): Unit = {
    val countState = ctx.getPartitionedState(
      new ValueStateDescriptor[Long]("count", classOf[Long])
    )
    countState.clear()
  }
}
