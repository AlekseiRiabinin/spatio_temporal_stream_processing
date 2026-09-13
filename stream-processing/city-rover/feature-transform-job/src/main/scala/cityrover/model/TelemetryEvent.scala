package cityrover.model


/**
  * Flink‑native internal representation of telemetry events.
  *
  * This mirrors the raw telemetry schema produced by the rover devices
  * and consumed from the Kafka "raw telemetry" topic.
  *
  * It replaces the ScalaPB-generated `Telemetry` message inside the pipeline
  * to ensure Flink derives efficient TypeInformation (no Kryo fallback),
  * improving latency and throughput.
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
