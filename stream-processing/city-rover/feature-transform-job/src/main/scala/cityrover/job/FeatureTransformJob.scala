package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

import cityrover.pipeline.FeatureProcessingPipeline
import cityrover.util.ConfigLoader
import cityrover.util.MetricsConfig


object FeatureTransformJob:

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    // Allow passing parallelism as CLI argument
    val parallelism =
      if args.nonEmpty then args(0).toInt
      else 1

    // Load the metrics configuration.
    // ConfigLoader is an object with typed accessors; `metrics` returns a
    // MetricsConfig case class parsed from the cityrover.metrics.* block
    // in application.conf.
    val metricsCfg: MetricsConfig = ConfigLoader.metrics

    log.info(
      s"Starting feature-transform-job: parallelism=$parallelism, " +
      s"metrics.namespace=${metricsCfg.namespace}, " +
      s"metrics.enabled=${metricsCfg.prometheusEnabled}"
    )

    // Sanity-check: log which latency stages will be registered.
    // Matches what the PyFlink probes print via their "READY" log line.
    val stages: Seq[String] =
      Seq(
        Option.when(metricsCfg.register.kafkaToFlink)("kafka_to_flink_ms"),
        Option.when(metricsCfg.register.queue)("queue_ms"),
        Option.when(metricsCfg.register.transform)("transform_ms"),
        Option.when(metricsCfg.register.emit)("emit_ms"),
        Option.when(metricsCfg.register.endToEnd)("latency_ms")
      ).flatten

    log.info(
      s"Registered latency metrics under group '${metricsCfg.metricGroup}': $stages"
    )

    // Create Flink environment
    val env: StreamExecutionEnvironment =
      FlinkEnvironment.create(parallelism)

    // Build the feature processing pipeline, injecting the metrics config.
    FeatureProcessingPipeline.build(env, metricsCfg)

    // Execute the job
    env.execute("cityrover-feature-transform-job")

end FeatureTransformJob
