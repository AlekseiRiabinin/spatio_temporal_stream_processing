package cityrover.util


object GeoUtils:

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double =
    val R = 6371e3
    val φ1 = math.toRadians(lat1)
    val φ2 = math.toRadians(lat2)
    val Δφ = math.toRadians(lat2 - lat1)
    val Δλ = math.toRadians(lon2 - lon1)
    val a =
      math.sin(Δφ / 2) * math.sin(Δφ / 2) +
      math.cos(φ1) * math.cos(φ2) *
      math.sin(Δλ / 2) * math.sin(Δλ / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R * c

  def computeGridCell(lat: Double, lon: Double): Long =
    ((lat * 100).toLong << 32) | (lon * 100).toLong

  def computeRegion(lat: Double, lon: Double): String =
    if lat > 25 && lon > 55 then "UAE" else "UNKNOWN"
