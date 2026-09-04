package cityrover.telemetry


/**
  * EnrichedTelemetryEvent is the domain model produced by the
  * ProcessingPipeline after transforming raw TelemetryEvent (protobuf)
  * and attaching latency profiling information.
  */
final case class EnrichedTelemetryEvent(
  roverId: String,
  ts: Long,
  lat: Double,
  lon: Double,
  speed: Double,
  heading: Double,
  latencyNs: Long
)
