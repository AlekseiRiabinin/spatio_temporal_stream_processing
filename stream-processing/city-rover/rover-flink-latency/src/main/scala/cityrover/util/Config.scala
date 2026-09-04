package cityrover.util

import com.typesafe.config.{Config, ConfigFactory}


object ConfigLoader:

  private val config: Config = ConfigFactory.load()

  // ------------------------------------------------------------
  // Kafka configuration
  // ------------------------------------------------------------

  def kafkaBootstrap: String =
    config.getString("cityrover.kafka.bootstrap")

  def kafkaRawTelemetryTopic: String =
    config.getString("cityrover.kafka.topics.raw-telemetry")

  // ------------------------------------------------------------
  // Pipeline configuration
  // ------------------------------------------------------------

  def watermarkDelayMs: Long =
    config.getLong("cityrover.pipeline.watermark-delay-ms")

  def windowSizeMs: Long =
    config.getLong("cityrover.pipeline.window-size-ms")

  // ------------------------------------------------------------
  // Latency profiler configuration
  // ------------------------------------------------------------

  def latencyProfilerEnabled: Boolean =
    config.getBoolean("cityrover.latency.profiler.enabled")

  def latencyProfilerSampleRate: Int =
    config.getInt("cityrover.latency.profiler.sample-rate")

  // ------------------------------------------------------------
  // Generic accessor (public)
  // ------------------------------------------------------------

  def rawConfig: Config = config

end ConfigLoader
