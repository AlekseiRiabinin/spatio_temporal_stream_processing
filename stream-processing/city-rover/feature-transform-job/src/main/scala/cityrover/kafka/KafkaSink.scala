package cityrover.kafka

import org.apache.flink.connector.kafka.sink.{KafkaSink => FlinkKafkaSink}
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema

import cityrover.util.ConfigLoader
import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * KafkaSinks
  *
  * Provides Kafka sinks for:
  *   - enriched telemetry (protobuf)
  *
  * Avoids naming collision with Flink's KafkaSink class.
  */
object KafkaSinks:

  /**
    * Sink for enriched telemetry (protobuf-based)
    */
  def enrichedTelemetrySink(): FlinkKafkaSink[EnrichedTelemetryEvent] =
    FlinkKafkaSink.builder[EnrichedTelemetryEvent]()
      .setBootstrapServers(ConfigLoader.kafkaBootstrap)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[EnrichedTelemetryEvent]()
          .setTopic(ConfigLoader.kafkaEnrichedTelemetryTopic)
          .setValueSerializationSchema(new EnrichedEventProtobufSchema())
          .build()
      )
      .build()
