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

  def kafkaEnrichedTelemetryTopic: String =
    config.getString("cityrover.kafka.topics.enriched-telemetry")

  def kafkaConsumerGroup: String =
    config.getString("cityrover.kafka.consumer-group")

  // ------------------------------------------------------------
  // Pipeline configuration
  // ------------------------------------------------------------

  def watermarkDelayMs: Long =
    config.getLong("cityrover.pipeline.watermark-delay-ms")

  // Optional: window sizes for ML feature store
  def window5s: Int =
    config.getInt("cityrover.pipeline.windows.window-5s.size-sec")

  def window30s: Int =
    config.getInt("cityrover.pipeline.windows.window-30s.size-sec")

  def window1m: Int =
    config.getInt("cityrover.pipeline.windows.window-1m.size-sec")

  def window5m: Int =
    config.getInt("cityrover.pipeline.windows.window-5m.size-sec")

  // ------------------------------------------------------------
  // Generic accessor (public)
  // ------------------------------------------------------------

  def rawConfig: Config = config

end ConfigLoader
