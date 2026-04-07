package phd.spatialmethods.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.Collector
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.eventtime.{
  WatermarkStrategy,
  SerializableTimestampAssigner
}
import java.time.Duration

import phd.spatialmethods.model.{GeoEvent, Interaction}


/**
 * SpatialStreamPipeline
 *
 * Bridges the abstract ProcessingGraph with Flink runtime.
 * Implements:
 *  - event-time processing
 *  - watermarking
 *  - windowed computation
 *  - interaction detection
 */
object SpatialStreamPipeline {

  /**
   * Builds the full streaming pipeline
   */
  def buildPipeline(
    env: StreamExecutionEnvironment,
    inputStream: DataStream[GeoEvent]
  ): DataStream[Interaction] = {

    // ------------------------------------------------------------------
    // 1. Implicit TypeInformation (needed for custom case class output)
    // ------------------------------------------------------------------
    implicit val interactionTypeInfo: TypeInformation[Interaction] = 
      createTypeInformation[Interaction]

    // ------------------------------------------------------------------
    // 2. Watermarks (event-time semantics)
    // ------------------------------------------------------------------
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[GeoEvent](Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[GeoEvent] {
        override def extractTimestamp(event: GeoEvent, recordTimestamp: Long): Long =
          event.eventTimeMillis
      })

    val timedStream = inputStream
      .assignTimestampsAndWatermarks(watermarkStrategy)

    // ------------------------------------------------------------------
    // 3. Global event-time window (no key)
    // ------------------------------------------------------------------
    val windowedStream = timedStream
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))

    // ------------------------------------------------------------------
    // 4. Initialize processing graph (core logic)
    // ------------------------------------------------------------------
    val processingGraph = new ProcessingGraph()

    // ------------------------------------------------------------------
    // 5. Apply spatial-temporal processing on each window batch
    // ------------------------------------------------------------------
    val interactionsStream: DataStream[Interaction] =
      windowedStream.apply { 
        (
          window: TimeWindow,
          elements: Iterable[GeoEvent],
          out: Collector[Interaction]
        ) =>
        val batch = elements.toSeq

        // Core scientific logic
        val interactions = processingGraph.process(batch)

        interactions.foreach(out.collect)
      }

    interactionsStream
  }
}
