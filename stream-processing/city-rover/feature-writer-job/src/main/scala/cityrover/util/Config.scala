package cityrover.util

import com.typesafe.config.{Config, ConfigFactory}


/**
  * ConfigLoader
  *
  * Loads configuration values for:
  *   - Kafka (enriched telemetry topic)
  *   - Cassandra (cluster + keyspace + table)
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
  // Generic accessor (public)
  // ------------------------------------------------------------

  def rawConfig: Config = config

end ConfigLoader
