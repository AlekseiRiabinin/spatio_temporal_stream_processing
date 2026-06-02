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
import phd.adaptivecontrol.adaptive.StreamProfiler


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

    // ------------------------------------------------------------
    // Select window size based on strategy
    // ------------------------------------------------------------
    val windowSizeMs: Long =
      config.windowStrategy match {

        case "adaptive" =>
          // Use adaptive value (updated dynamically)
          config.adaptiveWindowSizeMs

        case "fixed" | _ =>
          // Default: use configured fixed window
          config.windowSizeMs
      }

    println(
      s"[WINDOW PROCESSOR] action=config " +
      s"strategy=${config.windowStrategy} " +
      s"effectiveWindow=$windowSizeMs"
    )

    // ------------------------------------------------------------
    // Apply tumbling event-time window
    // ------------------------------------------------------------
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
              s"windowEnd=${context.window.getEnd} " +
              s"effectiveWindowSizeMs=$windowSizeMs"
            )

            // Update profiler window stats
            StreamProfiler.updateWindow(batch.size)

            out.collect(batch)
          }
        }
      )
  }
}
