package cityrover.kafka

import org.apache.flink.api.common.serialization.SerializationSchema
import cityrover.telemetry.EnrichedTelemetryEvent

/**
  * KafkaSchema
  *
  * Serializes EnrichedTelemetryEvent protobuf messages to raw bytes.
  * This is the fastest possible serialization strategy for Flink → Kafka.
  */
class EnrichedEventProtobufSchema extends SerializationSchema[EnrichedTelemetryEvent]:

  override def serialize(event: EnrichedTelemetryEvent): Array[Byte] =
    event.toByteArray
