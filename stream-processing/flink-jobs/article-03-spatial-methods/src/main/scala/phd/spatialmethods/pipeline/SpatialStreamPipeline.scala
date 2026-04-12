package phd.spatialmethods.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.eventtime.{
  WatermarkStrategy,
  SerializableTimestampAssigner
}

import java.time.Duration
import scala.collection.JavaConverters._

import phd.spatialmethods.model.{GeoEvent, Interaction}

object SpatialStreamPipeline {

  def buildPipeline(
      env: StreamExecutionEnvironment,
      inputStream: DataStream[GeoEvent]
  ): DataStream[Interaction] = {

    implicit val interactionTypeInfo: TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    // ------------------------------------------------------------------
    // Watermarks
    // ------------------------------------------------------------------
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[GeoEvent](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[GeoEvent] {
        override def extractTimestamp(
          event: GeoEvent,
          recordTimestamp: Long
        ): Long = {

          val ts = event.eventTimeMillis
          val now = System.currentTimeMillis()
          val lag = now - ts

          println(
            s"[TIMESTAMP] eventTime=$ts " +
            s"processingTime=$now " +
            s"lag=$lag " +
            s"objectId=${event.objectId}"
          )

          ts
        }
      })

    val timedStream = inputStream
      .assignTimestampsAndWatermarks(watermarkStrategy)

    // ------------------------------------------------------------------
    // Global event-time window
    // ------------------------------------------------------------------
    val windowedStream = timedStream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))

    // ------------------------------------------------------------------
    // Window function (anonymous class)
    // ------------------------------------------------------------------
    val interactionsStream: DataStream[Interaction] =
      windowedStream.apply { (window, elements, out: Collector[Interaction]) =>

        val batch = elements.toSeq
        val count = batch.size
        val windowStart = window.getStart
        val windowEnd = window.getEnd
        val now = System.currentTimeMillis()

        println(
          s"[WINDOW RESULT] windowStart=$windowStart windowEnd=$windowEnd " +
          s"count=$count processingTime=$now"
        )

        val processingGraph = new ProcessingGraph()
        val interactions = processingGraph.process(batch)

        interactions.foreach(out.collect)
      }

    interactionsStream
  }
}
