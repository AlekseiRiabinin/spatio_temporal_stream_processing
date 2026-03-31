package phd.spatialmethods.util


object GeometryUtils {

  private val EarthRadiusMeters = 6371000.0

  /**
   * Computes Haversine distance between two geo points (meters)
   */
  def haversineDistance(
    lat1: Double, lon1: Double,
    lat2: Double, lon2: Double
  ): Double = {

    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)

    val a =
      math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(math.toRadians(lat1)) *
      math.cos(math.toRadians(lat2)) *
      math.sin(dLon / 2) *
      math.sin(dLon / 2)

    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    EarthRadiusMeters * c
  }
}
