package cityrover.util

import com.typesafe.config.{Config, ConfigFactory}

import scala.jdk.CollectionConverters._


object ConfigLoader:

  private val config: Config = ConfigFactory.load()

  // ------------------------------------------------------------
  // Kafka configuration
  // ------------------------------------------------------------

  def kafkaBootstrap: String =
    config.getString("cityrover.kafka.bootstrap")

  def kafkaRawTelemetryTopic: String =
    config.getString("cityrover.kafka.topics.raw-telemetry")

  def kafkaEnrichedTelemetryTopic: String =
    config.getString("cityrover.kafka.topics.enriched-telemetry")

  def kafkaConsumerGroup: String =
    config.getString("cityrover.kafka.consumer-group")

  // ------------------------------------------------------------
  // Pipeline configuration
  // ------------------------------------------------------------

  def watermarkDelayMs: Long =
    config.getLong("cityrover.pipeline.watermark-delay-ms")

  def window5s: Int =
    config.getInt("cityrover.pipeline.windows.window-5s.size-sec")

  def window30s: Int =
    config.getInt("cityrover.pipeline.windows.window-30s.size-sec")

  def window1m: Int =
    config.getInt("cityrover.pipeline.windows.window-1m.size-sec")

  def window5m: Int =
    config.getInt("cityrover.pipeline.windows.window-5m.size-sec")

  // ------------------------------------------------------------
  // Metrics configuration
  // ------------------------------------------------------------
  //
  // Parses the cityrover.metrics.* block into a typed case class.
  // This is the value passed to LatencyMetrics so it knows which
  // histograms to register and under what Prometheus names.
  //
  // The metric set mirrors the Flink probes' latency decomposition:
  //   kafka_to_flink_ms, queue_ms, transform_ms, emit_ms, latency_ms
  //

  def metrics: MetricsConfig =
    val m = config.getConfig("cityrover.metrics")

    val prom   = m.getConfig("prometheus")
    val reg    = m.getConfig("register")
    val res    = m.getConfig("reservoir")
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
 * Mirrors the PyFlink metric names:
 *   kafkaToFlink -> kafka_to_flink_ms
 *   queue        -> queue_ms
 *   transform    -> transform_ms
 *   emit         -> emit_ms
 *   endToEnd     -> latency_ms
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
