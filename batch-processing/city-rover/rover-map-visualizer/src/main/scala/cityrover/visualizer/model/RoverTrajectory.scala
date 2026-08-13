package cityrover.visualizer.model

import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto._


/**
  * Represents a rover trajectory as returned by:
  *   GET /api/rovers/{id}/trajectory
  *
  * The Spark job produces GeoJSON:
  *
  * {
  *   "type": "Feature",
  *   "geometry": {
  *     "type": "LineString",
  *     "coordinates": [ [lon, lat], ... ]
  *   },
  *   "properties": {
  *     "roverId": "rover-1",
  *     ...
  *   }
  * }
  *
  * We keep the structure generic because GeoJSON is already
  * well‑defined and the frontend expects the raw GeoJSON object.
  */
case class RoverTrajectory(
  roverId: String,
  geojson: Json
)

object RoverTrajectory {
  implicit val encoder: Encoder[RoverTrajectory] = deriveEncoder[RoverTrajectory]
  implicit val decoder: Decoder[RoverTrajectory] = deriveDecoder[RoverTrajectory]
}
