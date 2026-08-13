package cityrover.visualizer.model

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._


/**
  * Replay data returned by:
  *   GET /api/rovers/{id}/replay
  *
  * Contains:
  *   - roverId
  *   - ordered list of RoverPosition samples
  */
case class RoverReplay(
  roverId: String,
  positions: Seq[RoverPosition]
)

object RoverReplay {
  implicit val encoder: Encoder[RoverReplay] = deriveEncoder[RoverReplay]
  implicit val decoder: Decoder[RoverReplay] = deriveDecoder[RoverReplay]
}
