package phd.architecture.util

import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.WKTReader


/**
 * Geometry utility functions.
 *
 * Responsible only for conversion and basic helpers.
 * No Flink, no Kafka, no business logic.
 */
object GeometryUtils {

  private val reader = new WKTReader()

  /**
   * Parse geometry from WKT string.
   *
   * Examples:
   *  - POINT(30 10)
   *  - LINESTRING(30 10, 40 40)
   *  - POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))
   */
  def fromWKT(wkt: String): Geometry =
    reader.read(wkt)

  /**
   * Returns a representative point for spatial indexing.
   * - POINT → itself
   * - Others → centroid
   */
  def representativePoint(geometry: Geometry): Point =
    geometry match {
      case p: Point => p
      case g => g.getCentroid
    }

  /**
   * Converts geometry to geohash using its representative point.
   */
  def toGeohash(geometry: Geometry, precision: Int): String = {
    val p = representativePoint(geometry)
    encodeGeohash(
      lat = p.getY,
      lon = p.getX,
      precision = precision
    )
  }

  // Placeholder (can be replaced with real library later)
  private def encodeGeohash(
      lat: Double,
      lon: Double,
      precision: Int
  ): String =
    f"$lat%.5f:$lon%.5f:$precision"

}
