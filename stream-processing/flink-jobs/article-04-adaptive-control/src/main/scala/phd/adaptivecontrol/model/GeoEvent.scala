package phd.adaptivecontrol.model


/**
  * GeoEvent
  *
  * Core spatio-temporal event model used by:
  *   - Kafka ingestion
  *   - Flink event-time processing
  *   - adaptive watermarking
  *   - adaptive window control
  *   - spatial interaction analysis
  *
  * IMPORTANT:
  * timestamp = EVENT TIME (not processing time)
  */
case class GeoEvent(

  // ============================================================
  // Event Identity
  // ============================================================
  id: String,

  // Moving object identifier
  objectId: String,

  // ============================================================
  // Temporal Attributes
  // ============================================================
  // Event-time timestamp (epoch millis)
  timestamp: Long,

  // ============================================================
  // Spatial Attributes
  // ============================================================
  lon: Double,
  lat: Double,

  // WKT geometry representation
  wkt: String,

  // ============================================================
  // Motion Attributes
  // ============================================================
  speed: Double,
  heading: Double,

  // ============================================================
  // Additional Metadata
  // ============================================================
  attributes: Map[String, String] = Map.empty
) {

  /**
    * Basic validation used during stream ingestion.
    */
    def isValid: Boolean = {

      (
        id.nonEmpty &&
        objectId.nonEmpty &&

        lon >= -180.0 &&
        lon <= 180.0 &&

        lat >= -90.0 &&
        lat <= 90.0 &&

        timestamp > 0
      )
    }

  /**
    * Convert event to JSON string.
    *
    * Useful for:
    *   - debugging
    *   - console output
    *   - Kafka sinks
    */
  def toJson: String = {

    val attrsJson =
      if (attributes.isEmpty) {
        ""
      } else {
        attributes
          .map { case (k, v) => s""""$k": "$v"""" }
          .mkString(", ")
      }

    s"""{
       |  "id": "$id",
       |  "objectId": "$objectId",
       |  "timestamp": $timestamp,
       |  "lon": $lon,
       |  "lat": $lat,
       |  "wkt": "$wkt",
       |  "speed": $speed,
       |  "heading": $heading,
       |  "attributes": { $attrsJson }
       |}""".stripMargin
  }
}
