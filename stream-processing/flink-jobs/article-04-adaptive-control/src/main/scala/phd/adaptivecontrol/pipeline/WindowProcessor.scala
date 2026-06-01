package phd.adaptivecontrol.pipeline

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import phd.adaptivecontrol.model.GeoEvent
import phd.adaptivecontrol.config.AdaptiveConfig


object WindowProcessor {

  // ------------------------------------------------------------
  // Explicit TypeInformation
  // ------------------------------------------------------------
  implicit val geoEventTypeInfo: TypeInformation[GeoEvent] =
    TypeInformation.of(classOf[GeoEvent])

  implicit val geoEventListTypeInfo: TypeInformation[List[GeoEvent]] =
    TypeInformation.of(classOf[List[GeoEvent]])

  // ------------------------------------------------------------
  // Window Processor
  // ------------------------------------------------------------
  def applyWindow(
    stream: DataStream[GeoEvent],
    config: AdaptiveConfig
  ): DataStream[List[GeoEvent]] = {

    val windowSizeMs = config.windowSizeMs

    println(
      s"[WINDOW PROCESSOR] action=config windowSizeMs=$windowSizeMs"
    )

    stream
      .keyBy(_ => "global")
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSizeMs)))
      .process(
        new ProcessWindowFunction[
          GeoEvent,
          List[GeoEvent],
          String,
          TimeWindow
        ] {

          override def process(
            key: String,
            context: Context,
            elements: Iterable[GeoEvent],
            out: Collector[List[GeoEvent]]
          ): Unit = {

            val batch = elements.toList

            println(
              s"[WINDOW PROCESSOR] key=$key " +
              s"events=${batch.size} " +
              s"windowStart=${context.window.getStart} " +
              s"windowEnd=${context.window.getEnd}"
            )

            out.collect(batch)
          }
        }
      )
  }
}
