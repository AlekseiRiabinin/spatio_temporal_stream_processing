package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import cityrover.pipeline.FeatureWriterPipeline
import cityrover.cassandra.CassandraConnectorConfig
import cityrover.util.{ConfigLoader, MetricsConfig}


object FeatureWriterJob:

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    // Determine parallelism (from env variable or default = 1)
    val parallelism =
      sys.env.get("FLINK_JOB_PARALLELISM").map(_.toInt).getOrElse(1)

    // ------------------------------------------------------------------
    // Load configuration.
    //
    // ConfigLoader reads application.conf and exposes:
    //   - rawConfig        : the whole Config tree (used for Cassandra)
    //   - metrics          : typed MetricsConfig parsed from
    //                        cityrover.metrics.*
    // ------------------------------------------------------------------
    val rawConfig    = ConfigLoader.rawConfig
    val metricsCfg   = ConfigLoader.metrics
    val cassandraCfg = CassandraConnectorConfig(rawConfig)

    log.info(
      s"Starting feature-writer-job: parallelism=$parallelism, " +
      s"metrics.namespace=${metricsCfg.namespace}, " +
      s"metrics.enabled=${metricsCfg.prometheusEnabled}"
    )

    // Sanity-check: log which latency stages will be registered.
    // Matches the PyFlink probe's "READY" log line so operators can
    // confirm the metrics config was picked up.
    val stages: Seq[String] =
      Seq(
        metricsCfg.register.kafkaToFlink -> "kafka_to_flink_ms",
        metricsCfg.register.queue        -> "flink_ms",
        metricsCfg.register.transform    -> "write_ms",
        metricsCfg.register.endToEnd     -> "latency_ms"
      ).collect { case (true, name) => name }

    log.info(
      s"Registered latency metrics under group '${metricsCfg.metricGroup}': $stages"
    )

    // ------------------------------------------------------------------
    // Create Flink environment (restart strategy, checkpointing, object reuse)
    // ------------------------------------------------------------------
    val env: StreamExecutionEnvironment =
      FlinkEnvironment.create(parallelism)

    // ------------------------------------------------------------------
    // Build pipeline: Kafka (protobuf enriched) → Cassandra
    // ------------------------------------------------------------------
    FeatureWriterPipeline.build(env, cassandraCfg, metricsCfg)

    // Execute job
    env.execute("cityrover-feature-writer")

end FeatureWriterJob
