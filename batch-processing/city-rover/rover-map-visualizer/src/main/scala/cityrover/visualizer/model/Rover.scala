package cityrover.visualizer.model

import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}


/**
  * Basic rover descriptor used in:
  *   - /api/rovers
  *   - sidebar list in the frontend
  *
  * Each rover corresponds to one GeoJSON file produced by
  * trajectory-visualizer-job.
  */
case class Rover(id: String)

object Rover {
  implicit val encoder: Encoder[Rover] = deriveEncoder[Rover]
  implicit val decoder: Decoder[Rover] = deriveDecoder[Rover]
}
