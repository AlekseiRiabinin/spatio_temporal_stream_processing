package cityrover.model

import java.time.Instant


case class GeoEvent(
  roverId: String,
  ts: Long,
  lat: Double,
  lon: Double,
  speed: Double,
  heading: Double,
  edgeId: Option[String],
  routeId: Option[String],
  eventTime: Instant,
  ingestionTime: Instant,
  processingStartNs: Option[Long] = None
)
