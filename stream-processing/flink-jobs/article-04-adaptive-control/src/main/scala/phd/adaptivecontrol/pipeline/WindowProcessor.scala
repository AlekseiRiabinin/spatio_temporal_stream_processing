package phd.adaptivecontrol.pipeline

import scala.collection.JavaConverters._

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import phd.adaptivecontrol.model.GeoEvent


object WindowProcessor {

  // ------------------------------------------------------------
  // Explicit TypeInformation
  // ------------------------------------------------------------
  implicit val stringTypeInfo: TypeInformation[String] =
    TypeInformation.of(classOf[String])

  implicit val geoEventTypeInfo: TypeInformation[GeoEvent] =
    TypeInformation.of(classOf[GeoEvent])

  implicit val geoEventListTypeInfo: TypeInformation[List[GeoEvent]] =
    TypeInformation.of(classOf[List[GeoEvent]])

  // ------------------------------------------------------------
  // Window Processor
  // ------------------------------------------------------------
  def applyWindow(stream: DataStream[GeoEvent]): DataStream[List[GeoEvent]] = {

    val windowSizeMs =
      sys.env
        .getOrElse("WINDOW_SIZE_MS", "5000")
        .toLong

    println(
      s"[WindowProcessor] windowSizeMs=$windowSizeMs"
    )

    stream
      .keyBy(_.objectId)
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

            val batch =
              elements.toList

            println(
              s"[WindowProcessor] key=$key " +
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
