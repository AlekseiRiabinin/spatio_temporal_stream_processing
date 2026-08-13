package cityrover.visualizer.model

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._


/**
  * Represents a single rover position sample used for replay:
  *
  * {
  *   "ts": 1691234567890,
  *   "lat": 25.2048,
  *   "lon": 55.2708,
  *   "speed": 10.0,
  *   "heading": 90.0,
  *   "edgeId": "12345",
  *   "routeId": "route-1"
  * }
  *
  * These objects come from the Spark job's "positions" array.
  */
case class RoverPosition(
  ts: Long,
  lat: Double,
  lon: Double,
  speed: Double,
  heading: Double,
  edgeId: Option[String],
  routeId: Option[String]
)

object RoverPosition {
  implicit val encoder: Encoder[RoverPosition] = deriveEncoder[RoverPosition]
  implicit val decoder: Decoder[RoverPosition] = deriveDecoder[RoverPosition]
}
