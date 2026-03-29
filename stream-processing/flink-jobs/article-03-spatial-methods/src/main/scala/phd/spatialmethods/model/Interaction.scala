package phd.spatialmethods.model

import java.time.Instant


/**
 * Interaction represents a spatio-temporal relation between two or more objects
 * (e.g., proximity, collision risk, swarm behavior).
 *
 * @param id              Unique interaction identifier
 * @param interactionType Type of interaction (collision, proximity, clustering, conflict)
 * @param objectIds       მონაწილating objects (rover IDs)
 * @param timestamp       Event time of interaction
 * @param locationWKT     Geometry where interaction occurred
 * @param severity        Optional severity score (e.g., risk level)
 * @param attributes      Additional metadata
 */
case class Interaction(
  id: String,
  interactionType: InteractionType,
  objectIds: Seq[String],
  timestamp: Instant,
  locationWKT: String,
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
  def eventTimeMillis: Long = timestamp.toEpochMilli

  /**
   * Basic validation
   */
  def isValid: Boolean =
    objectIds.nonEmpty &&
    locationWKT != null &&
    locationWKT.nonEmpty

  override def toString: String =
    s"Interaction(id=$id, type=$interactionType, participants=$participantsCount)"
}

/**
 * Enumeration of supported interaction types
 */
sealed trait InteractionType

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
