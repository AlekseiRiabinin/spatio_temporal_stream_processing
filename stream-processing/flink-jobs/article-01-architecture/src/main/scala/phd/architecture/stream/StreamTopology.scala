package phd.architecture.stream

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import java.nio.file.{Files, Paths, StandardOpenOption}

import phd.architecture.model.{Event, SpatialPartition, WindowResult}
import phd.architecture.operators.SpatialPartitionFunction
import phd.architecture.model.TypeInfos._


object StreamTopology {

  /**
   * Applies sliding event-time window with comprehensive debugging
   */
  def applySlidingWindow(
    stream: KeyedStream[Event, SpatialPartition],
    windowSizeSeconds: Int,
    slideSeconds: Int,
    geohashPrecision: Int,
    aggregation: ProcessWindowFunction[
      Event,
      WindowResult,
      SpatialPartition,
      TimeWindow
    ]
  ): DataStream[WindowResult] = {

    // DEBUG: Track events entering the window
    val trackedStream = stream.map { event =>
      println(s"📥 EVENT ENTERING WINDOW: id=${event.id}, time=${event.eventTime}")
      event
    }.name("WindowEntryTracker")
    
    // RE-KEY using the SAME precision from environment
    val rekeyedStream = trackedStream.keyBy(
      SpatialPartitionFunction.byGeohash(geohashPrecision)
    )

    // Apply windowing
    val windowedStream = rekeyedStream
      .window(
        SlidingProcessingTimeWindows.of(
          Time.seconds(windowSizeSeconds),
          Time.seconds(slideSeconds)
        )
      )
      .process(aggregation)
      .name("SlidingWindowAggregation")

    // DEBUG: Track results
    windowedStream.map { result =>
      println(s"📤 WINDOW RESULT PRODUCED: $result")
      result
    }.name("WindowExitTracker")
  }

  def sinkResults(results: DataStream[WindowResult]): DataStream[WindowResult] = {
    val debuggedStream = results.map { result =>
      println(
        s"📊 WINDOW RESULT SINK: " +
        s"windowStart=${result.windowStart}, " +
        s"windowEnd=${result.windowEnd}, " +
        s"geohash=${result.partition.geohash}, " +
        s"count=${result.value}"
      )
      
      // Write to a file in the container
      try {
        val output = 
          s"${System.currentTimeMillis()}," +
          s"${result.windowStart}," +
          s"${result.windowEnd}," +
          s"${result.partition.geohash}," +
          s"${result.value}\n"

        Files.write(
          Paths.get("/tmp/window_results.csv"),
          output.getBytes,
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
        )
      } catch {
        case e: Exception => 
          println(s"Error writing to file: ${e.getMessage}")
      }
      
      result
    }.name("DebugAndFileOutput")
    
    // Keep the original print sink as well
    debuggedStream.print().name("PrintSink")
    
    // Return the stream for potential further processing
    debuggedStream
  }
}
