package phd.adaptivecontrol.util


/**
  * GeometryUtils
  *
  * Spatial utility functions used for:
  *   - proximity detection
  *   - collision analysis
  *   - trajectory analysis
  *   - swarm density estimation
  *
  * IMPORTANT:
  * Distances are calculated in meters.
  */
object GeometryUtils {

  // ============================================================
  // Earth Radius
  // ============================================================
  private val EarthRadiusMeters =
    6371000.0

  // ============================================================
  // Haversine Distance
  // ============================================================
  /**
    * Computes great-circle distance
    * between two geographic coordinates.
    *
    * Result:
    *   distance in meters
    */
  def haversineDistance(
    lat1: Double,
    lon1: Double,
    lat2: Double,
    lon2: Double
  ): Double = {

    val dLat =
      math.toRadians(lat2 - lat1)

    val dLon =
      math.toRadians(lon2 - lon1)

    val a = (

      math.sin(dLat / 2) *
      math.sin(dLat / 2) +

      math.cos(math.toRadians(lat1)) *
      math.cos(math.toRadians(lat2)) *

      math.sin(dLon / 2) *
      math.sin(dLon / 2)

    )

    val c =
      2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a)
      )

    EarthRadiusMeters * c
  }

  // ============================================================
  // Midpoint Calculation
  // ============================================================
  /**
    * Computes midpoint between two coordinates.
    *
    * Useful for:
    *   - interaction center estimation
    *   - visualization
    *   - conflict localization
    */
  def midpoint(
    lat1: Double,
    lon1: Double,
    lat2: Double,
    lon2: Double
  ): (Double, Double) = {

    val midLat =
      (lat1 + lat2) / 2.0

    val midLon =
      (lon1 + lon2) / 2.0

    (midLat, midLon)
  }

  // ============================================================
  // Bounding Radius Check
  // ============================================================
  /**
    * Checks whether two objects are
    * within specified radius.
    */
  def withinRadius(
    lat1: Double,
    lon1: Double,
    lat2: Double,
    lon2: Double,
    radiusMeters: Double
  ): Boolean = {

    haversineDistance(
      lat1,
      lon1,
      lat2,
      lon2
    ) <= radiusMeters
  }
}
