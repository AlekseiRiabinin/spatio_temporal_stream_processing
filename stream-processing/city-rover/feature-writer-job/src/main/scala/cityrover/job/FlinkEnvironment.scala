package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.configuration.{Configuration, RestartStrategyOptions}
import java.time.Duration


object FlinkEnvironment:

  def create(parallelism: Int = 1): StreamExecutionEnvironment =
    val cfg = Configuration()

    // Restart strategy (Flink 2.x uses configuration-based restart strategies)
    cfg.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay")
    cfg.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3)
    cfg.set(
      RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY,
      Duration.ofSeconds(10)
    )

    // Create environment with configuration
    val env = StreamExecutionEnvironment.getExecutionEnvironment(cfg)

    // Parallelism
    env.setParallelism(parallelism)

    // Checkpointing (5 seconds)
    // Required for exactly-once Kafka → Cassandra delivery semantics
    env.enableCheckpointing(5000)

    // Reduce GC pressure (important for high-throughput telemetry)
    env.getConfig.enableObjectReuse()

    env

end FlinkEnvironment
