package phd.adaptivecontrol.model


/**
  * Interaction
  *
  * Represents a detected spatio-temporal interaction
  * between autonomous moving objects.
  *
  * Used by:
  *   - collision detection
  *   - proximity analysis
  *   - conflict prediction
  *   - swarm detection
  *
  * IMPORTANT:
  * timestamp = EVENT TIME
  */
case class Interaction(

  // ============================================================
  // Interaction Identity
  // ============================================================
  id: String,

  // Type of interaction
  interactionType: InteractionType,

  // Participating object IDs
  objectIds: Seq[String],

  // ============================================================
  // Temporal Attributes
  // ============================================================
  // Event-time timestamp
  timestamp: Long,

  // ============================================================
  // Spatial Attributes
  // ============================================================
  lat: Double,
  lon: Double,

  // ============================================================
  // Risk / Severity
  // ============================================================
  severity: Option[Double] = None,

  // ============================================================
  // Additional Metadata
  // ============================================================
  attributes: Map[String, String] = Map.empty
) {

  /**
    * Number of participating objects.
    */
  def participantsCount: Int =
    objectIds.size

  /**
    * Most common interaction case.
    */
  def isPairwise: Boolean =
    participantsCount == 2

  /**
    * Swarm/group interaction.
    */
  def isGroupInteraction: Boolean =
    participantsCount > 2

  /**
    * Flink-compatible event time.
    */
  def eventTimeMillis: Long =
    timestamp

  /**
    * WKT representation.
    */
  def toWKT: String =
    s"POINT($lon $lat)"

  /**
    * Basic validation.
    */
  def isValid: Boolean = {

    (
      objectIds.nonEmpty &&

      lat >= -90.0 &&
      lat <= 90.0 &&

      lon >= -180.0 &&
      lon <= 180.0 &&

      timestamp > 0
    )
  }

  /**
    * JSON export helper.
    */
  def toJson: String = {

    val objectsJson =
      objectIds
        .map(id => s""""$id"""")
        .mkString(", ")

    val attrsJson =
      if (attributes.isEmpty) {
        ""
      } else {
        attributes
          .map { case (k, v) => s""""$k": "$v"""" }
          .mkString(", ")
      }

    val severityJson =
      severity
        .map(_.toString)
        .getOrElse("null")

    s"""{
       |  "id": "$id",
       |  "interactionType": "$interactionType",
       |  "objectIds": [ $objectsJson ],
       |  "timestamp": $timestamp,
       |  "lat": $lat,
       |  "lon": $lon,
       |  "severity": $severityJson,
       |  "attributes": { $attrsJson }
       |}""".stripMargin
  }

  override def toString: String = {

    s"Interaction(" +
      s"id=$id, " +
      s"type=$interactionType, " +
      s"participants=$participantsCount, " +
      s"timestamp=$timestamp" +
      s")"
  }
}


/**
  * Supported interaction types.
  */
sealed trait InteractionType
  extends Serializable


object InteractionType {

  /**
    * Potential collision detected.
    */
  case object Collision extends InteractionType

  /**
    * Objects located within
    * spatial proximity threshold.
    */
  case object Proximity extends InteractionType

  /**
    * Swarm/group movement behavior.
    */
  case object Swarm extends InteractionType

  /**
    * Predicted trajectory conflict.
    */
  case object Conflict extends InteractionType
}
