package cityrover.connect


final case class TelemetryData(
  roverId: String,
  ts: Long,
  lat: Option[Double],
  lon: Option[Double],
  speed: Option[Double],
  heading: Option[Double],
  edgeId: Option[String],
  routeId: Option[String]
)
