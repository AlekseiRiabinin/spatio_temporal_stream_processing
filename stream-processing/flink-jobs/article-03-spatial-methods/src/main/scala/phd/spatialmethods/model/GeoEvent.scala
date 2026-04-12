package phd.spatialmethods.model


/**
 * GeoEvent represents a single spatio-temporal event in the stream.
 *
 * @param id         Unique event identifier
 * @param objectId   Identifier of the moving object (e.g., rover/drone)
 * @param timestamp  Event time in epoch milliseconds (event-time semantics)
 * @param lon        Longitude
 * @param lat        Latitude
 * @param wkt        Geometry in WKT format (Point/LineString/Polygon)
 * @param speed      Optional speed attribute (m/s)
 * @param heading    Optional direction (degrees)
 * @param attributes Additional dynamic attributes
 */
case class GeoEvent(
  id: String,
  objectId: String,
  timestamp: Long,
  lon: Double,
  lat: Double,
  wkt: String,
  speed: Option[Double] = None,
  heading: Option[Double] = None,
  attributes: Map[String, String] = Map.empty
) {

  /**
   * Returns event time in epoch milliseconds (for Flink compatibility)
   */
  def eventTimeMillis: Long = timestamp

  /**
   * Simple validation (can be extended for experiments)
   */
  def isValid: Boolean =
    lon >= -180 && lon <= 180 &&
    lat >= -90 && lat <= 90 &&
    wkt != null && wkt.nonEmpty

  /**
   * Lightweight representation for logging/debugging
   */
  override def toString: String =
    s"GeoEvent(id=$id, objectId=$objectId, ts=$timestamp, lon=$lon, lat=$lat)"
}
