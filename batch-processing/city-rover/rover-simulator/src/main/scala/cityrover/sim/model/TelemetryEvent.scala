package cityrover.sim.model


/**
  * Final telemetry event sent to Kafka.
  * This is the JSON payload consumed by Spark.
  */
case class TelemetryEvent(
  roverId: String,
  ts: Long,
  lat: Double,
  lon: Double,
  speed: Double,
  heading: Double,
  edgeId: String,
  routeId: String
)
