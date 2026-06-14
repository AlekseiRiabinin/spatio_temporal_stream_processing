package phd.adaptivecontrol.pipeline

import scala.collection.mutable.ListBuffer

import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.adaptive.AdaptiveRuntimeState


object AdaptiveWindowOperator {

  implicit val geoEventTypeInfo: TypeInformation[GeoEvent] =
    TypeInformation.of(classOf[GeoEvent])

  implicit val geoEventListTypeInfo: TypeInformation[List[GeoEvent]] =
    TypeInformation.of(classOf[List[GeoEvent]])

  def apply(
    stream: DataStream[GeoEvent],
    config: AdaptiveConfig
  ): DataStream[List[GeoEvent]] = {

    println(
      "[ADAPTIVE WINDOW] action=start " +
      s"initialWindowMs=${AdaptiveRuntimeState.windowSizeMs}"
    )

    stream
      .keyBy(_ => "global")
      .process(new AdaptiveWindowProcessFunction(config))
  }

  class AdaptiveWindowProcessFunction(config: AdaptiveConfig)
    extends KeyedProcessFunction[
      String,
      GeoEvent,
      List[GeoEvent]
    ] {

      @transient
      private var bufferState: ListState[GeoEvent] = _

      @transient
      private var timerState: ValueState[Long] = _

      override def open(parameters: Configuration): Unit = {

        bufferState =
          getRuntimeContext.getListState(
            new ListStateDescriptor[GeoEvent](
              "adaptive-window-buffer",
              classOf[GeoEvent]
            )
          )

        timerState =
          getRuntimeContext.getState(
            new ValueStateDescriptor[Long](
              "adaptive-window-timer",
              classOf[Long]
            )
          )
      }

      override def processElement(
        event: GeoEvent,
        ctx: KeyedProcessFunction[
          String,
          GeoEvent,
          List[GeoEvent]
        ]#Context,
        out: Collector[List[GeoEvent]]
      ): Unit = {

        bufferState.add(event)

        val currentTimer =
          timerState.value()

        if (currentTimer == 0L) {

          println(
            "[ADAPTIVE WINDOW] action=current_config " +
            s"windowMs=${AdaptiveRuntimeState.windowSizeMs}"
          )

          val windowSizeMs =
            math.max(
              1000L,
              AdaptiveRuntimeState.windowSizeMs
            )

          val triggerTs =
            event.timestamp + windowSizeMs

          ctx.timerService.registerEventTimeTimer(
            triggerTs
          )

          timerState.update(triggerTs)

          println(
            "[ADAPTIVE WINDOW] action=register " +
            s"windowMs=$windowSizeMs " +
            s"triggerTs=$triggerTs"
          )
        }
      }

      override def onTimer(
        timestamp: Long,
        ctx: KeyedProcessFunction[
          String,
          GeoEvent,
          List[GeoEvent]
        ]#OnTimerContext,
        out: Collector[List[GeoEvent]]
      ): Unit = {

        val events =
          ListBuffer.empty[GeoEvent]

        val iterator =
          bufferState.get().iterator()

        while (iterator.hasNext) {
          events += iterator.next()
        }

        val batch =
          events.toList

        println(
          "[ADAPTIVE WINDOW] action=emit " +
          s"events=${batch.size} " +
          s"windowMs=${AdaptiveRuntimeState.windowSizeMs} " +
          s"timerTs=$timestamp"
        )

        if (batch.nonEmpty) {
          out.collect(batch)
        }

        bufferState.clear()
        timerState.clear()
      }
    }
}
