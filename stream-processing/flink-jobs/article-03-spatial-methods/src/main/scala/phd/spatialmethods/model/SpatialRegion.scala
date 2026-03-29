package phd.spatialmethods.model


/**
 * SpatialRegion represents a spatial area used for filtering,
 * aggregation, and interaction analysis in streaming pipelines.
 *
 * @param id         Unique region identifier
 * @param name       Human-readable name
 * @param wkt        Geometry in WKT format (Polygon / MultiPolygon)
 * @param regionType Type of region (e.g., zone, geofence, hotspot)
 * @param attributes Additional metadata
 */
case class SpatialRegion(
  id: String,
  name: String,
  wkt: String,
  regionType: String,
  attributes: Map[String, String] = Map.empty
) {

  /**
   * Basic validation of region
   */
  def isValid: Boolean =
    wkt != null &&
    wkt.nonEmpty &&
    (wkt.startsWith("POLYGON") || wkt.startsWith("MULTIPOLYGON"))

  /**
   * Check if region is polygonal
   */
  def isPolygon: Boolean =
    wkt.startsWith("POLYGON")

  /**
   * Check if region is multipolygon
   */
  def isMultiPolygon: Boolean =
    wkt.startsWith("MULTIPOLYGON")

  /**
   * Lightweight representation for debugging/logging
   */
  override def toString: String =
    s"SpatialRegion(id=$id, name=$name, type=$regionType)"
}
