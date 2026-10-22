package cityrover.kafka

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.slf4j.LoggerFactory

import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * EnrichedEventSchema
  *
  * Deserializes protobuf bytes from Kafka into EnrichedTelemetryEvent.
  *
  * The transform job now emits one partial record per window firing:
  * exactly one `oneof features` variant is set, and `windowType` says
  * which one. This schema parses the message and validates that
  * `windowType` agrees with the populated variant, so a malformed
  * message (old-shape producer, cutover race, unknown window type) is
  * rejected here instead of flowing downstream as zeros or tombstones.
  *
  * Malformed messages are surfaced by throwing. Flink's restart
  * strategy decides what happens next (usually: fail the subtask and
  * restart). If you'd rather skip-and-count, route the exception to a
  * side output in FeatureWriterPipeline instead of throwing here.
  *
  * Used by FeatureWriterJob to read from rover-telemetry-enriched topic.
  */
class EnrichedEventSchema extends DeserializationSchema[EnrichedTelemetryEvent]:

  @transient private val log = LoggerFactory.getLogger(getClass)

  override def deserialize(message: Array[Byte]): EnrichedTelemetryEvent =
    val proto =
      try EnrichedTelemetryEvent.parseFrom(message)
      catch
        case e: Throwable =>
          throw new IllegalArgumentException(
            s"Failed to parse EnrichedTelemetryEvent (${message.length} bytes)", e
          )

    if proto.roverId.isEmpty then
      throw malformed(proto, "roverId is empty")

    val matches: Boolean =
      proto.features match
        case EnrichedTelemetryEvent.Features.W5S(_)  =>
          proto.windowType == cityrover.telemetry.WindowType.WINDOW_5S
        case EnrichedTelemetryEvent.Features.W30S(_) =>
          proto.windowType == cityrover.telemetry.WindowType.WINDOW_30S
        case EnrichedTelemetryEvent.Features.W1M(_)  =>
          proto.windowType == cityrover.telemetry.WindowType.WINDOW_1M
        case EnrichedTelemetryEvent.Features.W5M(_)  =>
          proto.windowType == cityrover.telemetry.WindowType.WINDOW_5M
        case EnrichedTelemetryEvent.Features.Empty =>
          false

    if !matches then
      throw malformed(
        proto,
        s"windowType=${proto.windowType} disagrees with features=${proto.features.getClass.getSimpleName}"
      )

    proto

  private def malformed(
    proto: EnrichedTelemetryEvent,
    reason: String
  ): IllegalArgumentException =
    val msg =
      s"Malformed EnrichedTelemetryEvent: $reason " +
      s"(roverId='${proto.roverId}', windowEndTs=${proto.windowEndTs})"
    log.warn(msg)
    new IllegalArgumentException(msg)

  override def isEndOfStream(nextElement: EnrichedTelemetryEvent): Boolean =
    false

  override def getProducedType: TypeInformation[EnrichedTelemetryEvent] =
    TypeInformation.of(classOf[EnrichedTelemetryEvent])
