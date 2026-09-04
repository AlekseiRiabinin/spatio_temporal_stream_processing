package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.configuration.Configuration

import cityrover.pipeline.ProcessingPipeline
import cityrover.metrics.PrometheusSetup
import cityrover.cassandra.CassandraConnectorConfig
import cityrover.util.ConfigLoader


object LatencyResearchJob:

  def main(args: Array[String]): Unit =
    // Prometheus metrics configuration
    val cfg: Configuration = PrometheusSetup.config

    // Flink environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment(cfg)

    // Cassandra configuration (from application.conf)
    val cassandraCfg = CassandraConnectorConfig(ConfigLoader.rawConfig)

    // Build full pipeline: Kafka → metrics → Cassandra
    ProcessingPipeline.build(env, cassandraCfg)

    // Execute job
    env.execute("cityrover-latency-research")

end LatencyResearchJob
