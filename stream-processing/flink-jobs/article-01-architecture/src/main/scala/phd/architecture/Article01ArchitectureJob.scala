package phd.architecture

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode

import phd.architecture.model._
import phd.architecture.stream._
import phd.architecture.operators._
import phd.architecture.util._
import phd.architecture.sink._


object Article01ArchitectureJob {

  def main(args: Array[String]): Unit = {

    // 1. Load configuration
    val parallelism = sys.env.getOrElse("FLINK_PARALLELISM", "1").toInt
    val maxOutOfOrderness = sys.env.getOrElse("MAX_OUT_OF_ORDERNESS", "5").toInt
    val geohashPrecision = sys.env.getOrElse("GEOHASH_PRECISION", "6").toInt
    val windowSize = sys.env.getOrElse("WINDOW_SIZE", "30").toInt
    val windowSlide = sys.env.getOrElse("WINDOW_SLIDE", "30").toInt

    // 2. Execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    env.enableCheckpointing(15000, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 10000))

    env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints")

    // 3. Kafka source
    val rawEvents: DataStream[Event] =
      KafkaSourceFactory.createSpatialEventSource(env)
        .name("KafkaSource")

    // 4. Watermarks
    val eventTimeStream: DataStream[Event] =
      rawEvents.assignTimestampsAndWatermarks(
        WatermarkStrategyFactory.eventTimeWatermarks(maxOutOfOrderness)
      ).name("AssignWatermarks")

    // 5. Spatial partitioning
    val partitionedStream: KeyedStream[Event, SpatialPartition] =
      eventTimeStream
        .name("AssignWatermarks")
        .keyBy(SpatialPartitionFunction.byGeohash(geohashPrecision))

    // 6. Windowed aggregation
    val windowedResults: DataStream[WindowResult] =
      StreamTopology.applySlidingWindow(
        partitionedStream,
        windowSize,
        windowSlide,
        geohashPrecision,
        WindowAggregation.countEvents
      ).name("SlidingWindowAggregation")

    // 7. Sinks
    StreamTopology.sinkResults(windowedResults)
    // windowedResults
    //   .addSink(new PostgresWindowResultSink())
    //   .name("PostgresSink")
    //   .uid("PostgresSink")

    // 8. Execute
    env.execute("Article 01 - Distributed Spatio-Temporal Architecture")
  }
}
