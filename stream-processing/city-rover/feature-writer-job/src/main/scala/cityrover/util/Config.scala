package cityrover.util

import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters._


/**
 * ConfigLoader
 *
 * Loads configuration values for:
 *   - Kafka (enriched telemetry topic)
 *   - Cassandra (cluster + keyspace + table)
 *   - Metrics (Prometheus namespace, latency stage toggles, reservoir)
 *
 * Matches the style of feature-transform-job ConfigLoader.
 */
object ConfigLoader:

  private val config: Config = ConfigFactory.load()

  // ------------------------------------------------------------
  // Kafka configuration
  // ------------------------------------------------------------

  def kafkaBootstrap: String =
    config.getString("cityrover.kafka.bootstrap")

  def kafkaEnrichedTelemetryTopic: String =
    config.getString("cityrover.kafka.topics.enriched-telemetry")

  def kafkaConsumerGroup: String =
    config.getString("cityrover.kafka.consumer-group")

  // ------------------------------------------------------------
  // Cassandra configuration
  // ------------------------------------------------------------

  def cassandraHost: String =
    config.getString("cityrover.cassandra.host")

  def cassandraPort: Int =
    config.getInt("cityrover.cassandra.port")

  def cassandraKeyspace: String =
    config.getString("cityrover.cassandra.keyspace")

  def cassandraTable: String =
    config.getString("cityrover.cassandra.table")

  // Optional writer tuning
  def cassandraWriteBatchSize: Int =
    config.getInt("cityrover.cassandra.write.batch-size")

  def cassandraWriteMaxRetries: Int =
    config.getInt("cityrover.cassandra.write.max-retries")

  // ------------------------------------------------------------
  // Metrics configuration
  // ------------------------------------------------------------
  //
  // Parses the cityrover.metrics.* block into a typed case class.
  // This is the value passed to LatencyMetrics so it knows which
  // histograms to register and under what Prometheus names.
  //
  // The metric set mirrors the PyFlink probe decomposition
  // (cassandra_latency_probe.py):
  //   kafka_to_flink_ms, flink_ms, write_ms, latency_ms
  //
  // Note: the writer job has no transform stage and no Kafka sink,
  // so the `transform` and `emit` toggles are expected to be false
  // in application.conf. The parser still reads them (so the case
  // class shape matches the transform job) but they default to false
  // when the config omits them.
  //

  def metrics: MetricsConfig =
    val m       = config.getConfig("cityrover.metrics")
    val prom    = m.getConfig("prometheus")
    val reg     = m.getConfig("register")
    val res     = m.getConfig("reservoir")
    val tagsCfg = m.getConfig("tags")

    val tags: Map[String, String] =
      tagsCfg.entrySet().asScala.map { entry =>
        entry.getKey -> tagsCfg.getString(entry.getKey)
      }.toMap

    MetricsConfig(
      prometheusEnabled = prom.getBoolean("enabled"),
      namespace         = prom.getString("namespace"),
      metricGroup       = m.getString("metric-group"),
      register = RegisterToggles(
        kafkaToFlink = reg.getBoolean("kafka-to-flink"),
        queue        = reg.getBoolean("queue"),
        transform    = reg.getBoolean("transform"),
        emit         = reg.getBoolean("emit"),
        endToEnd     = reg.getBoolean("end-to-end")
      ),
      reservoir = ReservoirConfig(
        kind       = res.getString("type"),
        windowSize = res.getInt("window-size")
      ),
      sampleRate = m.getDouble("sample-rate"),
      tags       = tags
    )

  // ------------------------------------------------------------
  // Generic accessor (public)
  // ------------------------------------------------------------

  def rawConfig: Config = config

end ConfigLoader


// ================================================================
// Typed configuration models
//
// These are intentionally identical to the ones in feature-transform-job
// so that LatencyMetrics.scala can be shared or duplicated between the
// two projects without modification.
// ================================================================

/**
 * Full metrics configuration parsed from cityrover.metrics.
 *
 * @param prometheusEnabled  whether to actually register metrics (false = no-op)
 * @param namespace          logical name used in logs and dashboards
 * @param metricGroup        Flink MetricGroup name (prefixes Prometheus metric names)
 * @param register           which latency stages to register as histograms
 * @param reservoir          Dropwizard reservoir configuration for percentile tracking
 * @param sampleRate         fraction of records to sample (1.0 = every record)
 * @param tags               extra Prometheus labels attached to every metric
 */
final case class MetricsConfig(
  prometheusEnabled: Boolean,
  namespace: String,
  metricGroup: String,
  register: RegisterToggles,
  reservoir: ReservoirConfig,
  sampleRate: Double,
  tags: Map[String, String]
)

/**
 * Which latency stages to register.
 *
 * For the transform job:
 *   kafkaToFlink -> kafka_to_flink_ms
 *   queue        -> queue_ms
 *   transform    -> transform_ms
 *   emit         -> emit_ms
 *   endToEnd     -> latency_ms
 *
 * For the writer job:
 *   kafkaToFlink -> kafka_to_flink_ms
 *   queue        -> flink_ms          (same stage, different metric name)
 *   transform    -> (unused, set false)
 *   emit         -> (unused, set false)
 *   endToEnd     -> latency_ms
 *
 * The `LatencyMetrics` registry is free to rename the metric per job —
 * see its `recordQueue` / `recordTransform` methods.
 */
final case class RegisterToggles(
  kafkaToFlink: Boolean,
  queue: Boolean,
  transform: Boolean,
  emit: Boolean,
  endToEnd: Boolean
)

/**
 * Dropwizard reservoir configuration.
 *
 * @param kind       "sliding-window" (accurate percentiles, more memory)
 *                   or "exponentially-decaying" (cheaper, biased to recent)
 * @param windowSize number of samples retained for percentile calculations
 */
final case class ReservoirConfig(
  kind: String,
  windowSize: Int
)
