package phd.streammodels.window

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration

import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.model.TypeInfos._


class AdaptiveWindowStrategy[K : TypeInformation](
  baseWindowSeconds: Long,
  keySelector: Event => K
) extends WindowStrategy[K] {

  override val name: String = "adaptive"

  override def applyWindow(
    stream: DataStream[Event]
  ): DataStream[WindowResult[K]] = {

    // Step 1: compute adaptive window metadata
    val adaptiveStream: DataStream[WindowResult[K]] =
      stream
        .keyBy(keySelector)
        .process(new AdaptiveWindowSizer[K](baseWindowSeconds))

    // Step 2: explicit key selector for WindowResult[K]
    val resultKeySelector: WindowResult[K] => K =
      (wr: WindowResult[K]) => wr.partition

    // Step 3: final tumbling window using the adaptive metadata
    adaptiveStream
      .keyBy(resultKeySelector)
      .window(TumblingEventTimeWindows.of(Time.seconds(baseWindowSeconds)))
      .apply { (
        key: K,
        window: TimeWindow,
        input: Iterable[WindowResult[K]],
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
  * Computes density per key and emits events annotated with
  * an adjusted window size.
  */
class AdaptiveWindowSizer[K](baseWindowSeconds: Long)
  extends KeyedProcessFunction[K, Event, WindowResult[K]] {

  private var countState: ValueState[Long] = _
  private var lastUpdateState: ValueState[Long] = _

  override def open(parameters: Configuration): Unit = {
    countState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("count", classOf[Long])
    )
    lastUpdateState = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("lastUpdate", classOf[Long])
    )
  }

  override def processElement(
      event: Event,
      ctx: KeyedProcessFunction[K, Event, WindowResult[K]]#Context,
      out: Collector[WindowResult[K]]
  ): Unit = {

    val now = ctx.timerService().currentProcessingTime()

    val last = Option(lastUpdateState.value()).getOrElse(now)
    val count = Option(countState.value()).getOrElse(0L) + 1

    val delta = Math.max(1, now - last)
    val density = count.toDouble / delta.toDouble

    val adjustedSeconds = Math.max(1, (baseWindowSeconds / (1.0 + density)).toLong)

    out.collect(
      WindowResult(
        partition = ctx.getCurrentKey,
        windowStart = now,
        windowEnd = now + adjustedSeconds * 1000,
        value = count,
        processingTime = Some(now)
      )
    )

    countState.update(count)
    lastUpdateState.update(now)
  }
}
