package cityrover.kafka

import org.apache.flink.connector.kafka.source.KafkaSource
import cityrover.util.ConfigLoader
import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * KafkaSourceEnriched
  *
  * Provides a Kafka source for reading enriched telemetry events
  * in protobuf format from rover-telemetry-enriched topic.
  *
  * Used by FeatureWriterJob.
  */
object KafkaSourceEnriched:

  def source(): KafkaSource[EnrichedTelemetryEvent] =
    KafkaSource.builder[EnrichedTelemetryEvent]()
      .setBootstrapServers(ConfigLoader.kafkaBootstrap)
      .setTopics(ConfigLoader.kafkaEnrichedTelemetryTopic)
      .setGroupId(ConfigLoader.kafkaConsumerGroup + "-writer")
      .setValueOnlyDeserializer(new EnrichedEventSchema())
      .build()
