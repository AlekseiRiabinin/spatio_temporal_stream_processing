package cityrover.telemetry


/**
  * Flink‑native internal representation of telemetry events.
  *
  * This replaces ScalaPB's generated `Telemetry` message inside the pipeline.
  * It is a simple case class → Flink derives efficient TypeInformation,
  * avoiding Kryo fallback and improving latency + throughput.
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
