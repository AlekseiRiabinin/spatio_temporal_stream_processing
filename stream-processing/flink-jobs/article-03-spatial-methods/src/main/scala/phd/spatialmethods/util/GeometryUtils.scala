package phd.spatialmethods.util

import phd.spatialmethods.model.GeoEvent


object GeometryUtils {

  private val EarthRadiusMeters = 6371000.0 // average Earth radius in meters

  /** 
   * Compute the Haversine distance between two points specified by latitude and longitude in meters.
   */
  def haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val rLat1 = math.toRadians(lat1)
    val rLat2 = math.toRadians(lat2)

    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
            math.cos(rLat1) * math.cos(rLat2) *
            math.sin(dLon / 2) * math.sin(dLon / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    EarthRadiusMeters * c
  }

  /**
   * Check if a point is within a rectangular bounding box
   */
  def pointInBounds(
    lat: Double,
    lon: Double,
    minLat: Double,
    minLon: Double,
    maxLat: Double,
    maxLon: Double
  ): Boolean = {
    lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon
  }

  /**
   * Compute centroid of a list of GeoEvents
   */
  def centroid(events: Seq[GeoEvent]): Option[(Double, Double)] = {
    if (events.isEmpty) None
    else {
      val (sumLat, sumLon) = events.foldLeft((0.0, 0.0)) { case ((slat, slon), e) =>
        (slat + e.lat, slon + e.lon)
      }
      Some((sumLat / events.size, sumLon / events.size))
    }
  }

  /**
   * Compute Euclidean distance between two points (approximation, good for small areas)
   */
  def euclideanDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = lat2 - lat1
    val dLon = lon2 - lon1
    math.sqrt(dLat * dLat + dLon * dLon) * (Math.PI / 180.0 * EarthRadiusMeters)
  }

}
