// defines the pipeline topology
// explicitly reflects figures and formulas in the article
// uses event-time, windows, partitions

package phd.architecture

import org.apache.flink.streaming.api.scala._

import phd.architecture.model._
import phd.architecture.stream._
import phd.architecture.operators._
import phd.architecture.util._


object Article01ArchitectureJob {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------------
    // 1. Execution environment
    // ------------------------------------------------------------------
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // ------------------------------------------------------------------
    // 2. Source: spatial-temporal events from Kafka
    // ------------------------------------------------------------------
    val rawEvents: DataStream[Event] =
      KafkaSourceFactory.createSpatialEventSource(env)

    // ------------------------------------------------------------------
    // 3. Event-time semantics and watermarks
    // ------------------------------------------------------------------
    val eventTimeStream: DataStream[Event] =
      rawEvents.assignTimestampsAndWatermarks(
        WatermarkStrategyFactory.eventTimeWatermarks(
          maxOutOfOrdernessSeconds = 10
        )
      )

    // ------------------------------------------------------------------
    // 4. Spatial partitioning
    //    π(e) → p
    // ------------------------------------------------------------------
    val partitionedStream: KeyedStream[Event, SpatialPartition] =
      eventTimeStream.keyBy(
        SpatialPartitionFunction.byGeohash(precision = 7)
      )

    // ------------------------------------------------------------------
    // 5. Windowed aggregation
    //    Ω(W, p)
    // ------------------------------------------------------------------
    val windowedResults: DataStream[WindowResult] =
      StreamTopology.applySlidingWindow(
        stream = partitionedStream,
        windowSizeSeconds = 60,
        slideSeconds = 10,
        aggregation = WindowAggregation.countEvents
      )

    // ------------------------------------------------------------------
    // 6. Metrics collection
    // ------------------------------------------------------------------
    val measuredResults: DataStream[WindowResult] =
      LatencyMetrics.attachProcessingLatency(windowedResults)

    // ------------------------------------------------------------------
    // 7. Sink (e.g., PostGIS, logs, or Kafka for experiments)
    // ------------------------------------------------------------------
    StreamTopology.sinkResults(measuredResults)

    // ------------------------------------------------------------------
    // 8. Execute
    // ------------------------------------------------------------------
    env.execute("Article 01 - Distributed Spatio-Temporal Architecture")
  }
}
