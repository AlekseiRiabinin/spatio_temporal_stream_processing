// defines the pipeline topology
// explicitly reflects figures and formulas in the article
// uses event-time, windows, partitions

package phd.architecture

import org.apache.flink.streaming.api.scala._

import phd.architecture.model._
import phd.architecture.stream._
import phd.architecture.operators._
import phd.architecture.util._
import phd.architecture.metrics._


object Article01ArchitectureJob {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------------
    // 1. Load configuration from environment variables
    // ------------------------------------------------------------------
    val parallelism = sys.env.getOrElse("FLINK_PARALLELISM", "1").toInt
    val maxOutOfOrderness = sys.env.getOrElse("MAX_OUT_OF_ORDERNESS", "5").toInt
    val geohashPrecision = sys.env.getOrElse("GEOHASH_PRECISION", "6").toInt
    val windowSize = sys.env.getOrElse("WINDOW_SIZE", "30").toInt
    val windowSlide = sys.env.getOrElse("WINDOW_SLIDE", "30").toInt

    // ------------------------------------------------------------------
    // 2. Start metrics HTTP server
    // ------------------------------------------------------------------
    MetricsHttpServer.start(9000)

    // ------------------------------------------------------------------
    // 3. Execution environment
    // ------------------------------------------------------------------
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // ------------------------------------------------------------------
    // 4. Source: spatial-temporal events from Kafka
    // ------------------------------------------------------------------
    val rawEvents: DataStream[Event] =
      KafkaSourceFactory.createSpatialEventSource(env)

    // ------------------------------------------------------------------
    // 5. Event-time semantics and watermarks
    // ------------------------------------------------------------------
    val eventTimeStream: DataStream[Event] =
      rawEvents.assignTimestampsAndWatermarks(
        WatermarkStrategyFactory.eventTimeWatermarks(
          maxOutOfOrdernessSeconds = maxOutOfOrderness
        )
      )

    // ------------------------------------------------------------------
    // 6. Spatial partitioning
    //    π(e) → p
    // ------------------------------------------------------------------
    val partitionedStream: KeyedStream[Event, SpatialPartition] =
      eventTimeStream.keyBy(
        SpatialPartitionFunction.byGeohash(precision = geohashPrecision)
      )

    // ------------------------------------------------------------------
    // 7. Windowed aggregation
    //    Ω(W, p)
    // ------------------------------------------------------------------
    val windowedResults: DataStream[WindowResult] =
      StreamTopology.applySlidingWindow(
        stream = partitionedStream,
        windowSizeSeconds = windowSize,
        slideSeconds = windowSlide,
        aggregation = WindowAggregation.countEvents
      )

    // ------------------------------------------------------------------
    // 8. Metrics collection
    // ------------------------------------------------------------------
    val measuredResults: DataStream[WindowResult] =
      LatencyMetrics.attachProcessingLatency(windowedResults)

    // ------------------------------------------------------------------
    // 9. Sink (e.g., PostGIS, logs, or Kafka for experiments)
    // ------------------------------------------------------------------
    StreamTopology.sinkResults(measuredResults)

    // ------------------------------------------------------------------
    // 10. Execute
    // ------------------------------------------------------------------
    env.execute("Article 01 - Distributed Spatio-Temporal Architecture")
  }
}
