package cityrover.kafka

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types

import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * EnrichedEventSchema
  *
  * Deserializes protobuf bytes from Kafka into EnrichedTelemetryEvent.
  * This is the fastest possible strategy for Flink → Kafka → Flink pipelines.
  *
  * Used by FeatureWriterJob to read from rover-telemetry-enriched topic.
  */
class EnrichedEventSchema extends DeserializationSchema[EnrichedTelemetryEvent]:

  override def deserialize(message: Array[Byte]): EnrichedTelemetryEvent =
    EnrichedTelemetryEvent.parseFrom(message)

  override def isEndOfStream(nextElement: EnrichedTelemetryEvent): Boolean =
    false

  override def getProducedType: TypeInformation[EnrichedTelemetryEvent] =
    Types.POJO(classOf[EnrichedTelemetryEvent])
