package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.RestartStrategyOptions
import java.time.Duration


object FlinkEnvironment:

  def create(parallelism: Int = 1): StreamExecutionEnvironment =
    val cfg = Configuration()
    cfg.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay")
    cfg.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3)
    cfg.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10))

    val env = StreamExecutionEnvironment.getExecutionEnvironment(cfg)

    env.setParallelism(parallelism)
    env.enableCheckpointing(5000)
    env.getConfig.enableObjectReuse()

    env

end FlinkEnvironment
