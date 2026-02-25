package phd.streammodels

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.streaming.api.CheckpointingMode

import phd.streammodels.model._
import phd.streammodels.stream._
import phd.streammodels.windows._
import phd.streammodels.algorithms._
import phd.streammodels.util._


object Article02StreamModelsJob {

  def main(args: Array[String]): Unit = {

    // 1. Load configuration
    val parallelism = sys.env.getOrElse("FLINK_PARALLELISM", "1").toInt
    val modelName = sys.env.getOrElse("STREAM_MODEL", "dataflow").toLowerCase
    val strategyName = sys.env.getOrElse("WINDOW_STRATEGY", "adaptive").toLowerCase

    val baseWindowSeconds = sys.env.getOrElse("WINDOW_SIZE", "30").toLong
    val countThreshold = sys.env.getOrElse("COUNT_THRESHOLD", "100").toInt
    val processingIntervalMs = sys.env.getOrElse("PROCESSING_INTERVAL_MS", "5000").toLong
    val densityFactor = sys.env.getOrElse("DENSITY_FACTOR", "1.0").toDouble

    // 2. Execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    env.enableCheckpointing(15000, CheckpointingMode.EXACTLY_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 10000))

    env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoints")

    // 3. Kafka source (generic event stream)
    val rawEvents: DataStream[Event] = 
      KafkaSourceFactory
        .createEventSource(env)
        .name("KafkaSource")

    // 4. Select StreamModel
    val model: StreamModel[String] = modelName match {
      case "dataflow" =>
        new DataflowModel[String](env)

      case "microbatch" =>
        new MicroBatchModel[String](env)

      case "actor" =>
        new ActorModel[String](env)

      case "log" =>
        new LogModel[String](env)

      case other =>
        throw new IllegalArgumentException(s"Unknown model: $other")
    }

    // 5. Key selector for events
    val eventKeySelector: Event => String = e => e.id

    // 6. Select WindowStrategy
    val strategy: WindowStrategy[String] = strategyName match {
      case "session" =>
        new SessionWindowStrategy[String](
          sessionGapSeconds = baseWindowSeconds,
          keySelector = eventKeySelector
        )

      case "dynamic" =>
        new DynamicWindowStrategy[String](
          baseWindowSeconds = baseWindowSeconds,
          densityFactor = densityFactor,
          keySelector = eventKeySelector
        )

      case "adaptive" =>
        new AdaptiveWindowStrategy[String](
          baseWindowSeconds = baseWindowSeconds,
          keySelector = eventKeySelector
        )

      case "multitrigger" =>
        new MultiTriggerWindowStrategy[String](
          windowSeconds = baseWindowSeconds,
          keySelector = eventKeySelector,
          countThreshold = countThreshold,
          processingIntervalMs = processingIntervalMs
        )

      case other =>
        throw new IllegalArgumentException(s"Unknown window strategy: $other")
    }


    // 7. Build pipeline via StreamModel
    val windowedResults: DataStream[WindowResult[String]] =
      model
        .buildPipeline(rawEvents, strategy)
        .name("WindowedAggregation")

    // 8. Sink results
    StreamTopology.sinkResults(windowedResults)

    // 9. Execute
    env.execute(s"Article 02 - Stream Models & Window Strategies ($modelName, $strategyName)")
  }
}
