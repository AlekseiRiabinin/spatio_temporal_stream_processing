package phd.spatialmethods.model


/**
 * Interaction represents a spatio-temporal relation between two or more objects
 * (e.g., proximity, collision risk, swarm behavior).
 *
 * @param id              Unique interaction identifier
 * @param interactionType Type of interaction (collision, proximity, clustering, conflict)
 * @param objectIds       Participating objects (rover IDs)
 * @param timestamp       Event time in epoch milliseconds
 * @param lat             Latitude of interaction center
 * @param lon             Longitude of interaction center
 * @param severity        Optional severity score (e.g., risk level)
 * @param attributes      Additional metadata
 */
case class Interaction(
  id: String,
  interactionType: InteractionType,
  objectIds: Seq[String],
  timestamp: Long,
  lat: Double,
  lon: Double,
  severity: Option[Double] = None,
  attributes: Map[String, String] = Map.empty
) {

  /**
   * Number of participants in interaction
   */
  def participantsCount: Int = objectIds.size

  /**
   * Check if interaction is pairwise (most common case)
   */
  def isPairwise: Boolean = participantsCount == 2

  /**
   * Check if interaction is group-based (swarm behavior)
   */
  def isGroupInteraction: Boolean = participantsCount > 2

  /**
   * Event time in millis (for Flink compatibility)
   */
  def eventTimeMillis: Long = timestamp

  /**
   * Convert to WKT (optional, for storage/export)
   */
  def toWKT: String = s"POINT($lon $lat)"

  /**
   * Basic validation
   */
  def isValid: Boolean =
    objectIds.nonEmpty &&
    lat >= -90 && lat <= 90 &&
    lon >= -180 && lon <= 180

  override def toString: String =
    s"Interaction(id=$id, type=$interactionType, ts=$timestamp, participants=$participantsCount, lat=$lat, lon=$lon)"
}


/**
 * Enumeration of supported interaction types
 */
sealed trait InteractionType extends Serializable

object InteractionType {

  /** Potential collision detected */
  case object Collision extends InteractionType

  /** Objects are within a distance threshold */
  case object Proximity extends InteractionType

  /** Group behavior / clustering */
  case object Swarm extends InteractionType

  /** Conflict situation (e.g., path intersection with risk) */
  case object Conflict extends InteractionType

}
