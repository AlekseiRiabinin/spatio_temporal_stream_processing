package phd.streammodels.windows

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.model.TypeInfos._


/**
  * Multi-trigger window strategy:
  * - Uses tumbling event-time windows
  * - Fires on multiple conditions:
  *     1. Event-time (normal window end)
  *     2. Processing-time periodic trigger
  *     3. Count-based trigger
  *
  * This allows early results, late results, and periodic updates.
  */
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
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
      .trigger(
        new MultiTrigger[K](
          countThreshold = countThreshold,
          processingIntervalMs = processingIntervalMs
        )
      )
      .apply(new MultiTriggerWindowFunction[K])
  }
}

/**
  * A composite trigger that fires when:
  *  - the window ends (event-time)
  *  - count threshold is reached
  *  - periodic processing-time interval passes
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

    // Count-based trigger
    val countState = ctx.getPartitionedState(
      new ValueStateDescriptor[Long]("count", classOf[Long])
    )

    val newCount = Option(countState.value()).getOrElse(0L) + 1
    countState.update(newCount)

    if (newCount >= countThreshold) {
      countState.clear()
      return TriggerResult.FIRE
    }

    // Register processing-time timer for periodic firing
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

/**
  * Produces a WindowResult[K] when the trigger fires.
  */
class MultiTriggerWindowFunction[K]
    extends org.apache.flink.streaming.api.scala.function.WindowFunction[
      Event,
      WindowResult[K],
      K,
      TimeWindow
    ] {

  override def apply(
      key: K,
      window: TimeWindow,
      input: Iterable[Event],
      out: Collector[WindowResult[K]]
  ): Unit = {

    val count = input.size

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
