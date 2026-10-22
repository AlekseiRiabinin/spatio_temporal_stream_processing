package cityrover.kafka

import org.apache.flink.connector.kafka.sink.{KafkaSink => FlinkKafkaSink}
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.api.common.serialization.SerializationSchema

import java.nio.charset.StandardCharsets

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
    * Sink for enriched telemetry (protobuf-based).
    *
    * The Kafka message key is set to `roverId` so that every record for
    * a given rover lands in the same partition. This preserves
    * per-rover ordering (within the partition) and spreads load across
    * partitions. Without an explicit key, Kafka's default partitioner
    * hashes on a null key and distributes round-robin — no per-rover
    * ordering.
    * 
    * Idempotent Cassandra upserts will have USING TIMESTAMP = windowEndTs * 1000,
    * so DeliveryGuarantee.AT_LEAST_ONCE is used here. 
    */
  def enrichedTelemetrySink(): FlinkKafkaSink[EnrichedTelemetryEvent] =
    FlinkKafkaSink.builder[EnrichedTelemetryEvent]()
      .setBootstrapServers(ConfigLoader.kafkaBootstrap)
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) 
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[EnrichedTelemetryEvent]()
          .setTopic(ConfigLoader.kafkaEnrichedTelemetryTopic)
          .setKeySerializationSchema(new RoverIdKeySchema())
          .setValueSerializationSchema(new EnrichedEventProtobufSchema())
          .build()
      )
      .build()


/**
  * Serializes `EnrichedTelemetryEvent.roverId` as the Kafka message key
  * (UTF-8 bytes). Keeping the key schema in its own class makes it
  * obvious at the call site that the key is `roverId` and nothing else.
  */
class RoverIdKeySchema extends SerializationSchema[EnrichedTelemetryEvent]:

  override def serialize(event: EnrichedTelemetryEvent): Array[Byte] =
    event.roverId.getBytes(StandardCharsets.UTF_8)
