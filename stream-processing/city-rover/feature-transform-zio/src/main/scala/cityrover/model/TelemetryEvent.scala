package cityrover.model


/** Domain representation of one raw telemetry event.
  *
  * Produced by parsing a `cityrover.telemetry.Telemetry` protobuf message
  * from the raw telemetry topic. Mirrors the Flink job's
  * `cityrover.model.TelemetryEvent` field-for-field so the two jobs stay
  * in lockstep on the wire format.
  *
  * Fields `lat`, `lon`, `speed`, `heading`, `edgeId`, `routeId` are
  * optional in the proto (`optional double` / `optional string`) and are
  * defaulted to `0.0` / `""` on parse, matching the Flink job's
  * `proto.lat.getOrElse(0.0)` convention.
  */
final case class TelemetryEvent(
  roverId: String,
  lat: Double,
  lon: Double,
  ts: Long,
  speed: Double,
  heading: Double,
  edgeId: String,
  routeId: String
)
