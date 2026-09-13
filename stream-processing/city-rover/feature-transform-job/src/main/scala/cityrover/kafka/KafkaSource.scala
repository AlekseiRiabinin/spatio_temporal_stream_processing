package cityrover.kafka

import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.api.common.serialization.SimpleStringSchema

import cityrover.serialization.ByteArraySchema
import cityrover.util.ConfigLoader


/**
  * KafkaSources
  *
  * Provides Kafka sources for:
  *   - raw telemetry (protobuf bytes)
  *   - enriched telemetry (string-based)
  *
  * Matches the style of the previous Flink job exactly.
  */
object KafkaSources:

  /**
    * Raw telemetry source (protobuf bytes)
    */
  def rawTelemetrySource(): KafkaSource[Array[Byte]] =
    KafkaSource.builder[Array[Byte]]()
      .setBootstrapServers(ConfigLoader.kafkaBootstrap)
      .setTopics(ConfigLoader.kafkaRawTelemetryTopic)
      .setGroupId(ConfigLoader.kafkaConsumerGroup)
      .setValueOnlyDeserializer(ByteArraySchema())
      .build()

  /**
    * Enriched telemetry source (string-based)
    */
  def enrichedTelemetrySource(): KafkaSource[String] =
    KafkaSource.builder[String]()
      .setBootstrapServers(ConfigLoader.kafkaBootstrap)
      .setTopics(ConfigLoader.kafkaEnrichedTelemetryTopic)
      .setGroupId(ConfigLoader.kafkaConsumerGroup + "-enriched")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
