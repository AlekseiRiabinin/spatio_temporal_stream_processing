package phd.adaptivecontrol.producer.util


object GeoUtils {

  val EarthRadiusMeters: Double = 6371000.0


  def deg2rad(degrees: Double): Double = {
    degrees * math.Pi / 180.0
  }


  def rad2deg(radians: Double): Double = {
    radians * 180.0 / math.Pi
  }


  def moveCoordinate(
    lon: Double,
    lat: Double,
    headingDegrees: Double,
    distanceMeters: Double
  ): (Double, Double) = {

    val headingRad =
      deg2rad(headingDegrees)

    val dLat =
      (distanceMeters * math.cos(headingRad)) / EarthRadiusMeters

    val dLon =
      (distanceMeters * math.sin(headingRad)) / (EarthRadiusMeters * math.cos(deg2rad(lat)))

    val newLat =
      lat + rad2deg(dLat)

    val newLon =
      lon + rad2deg(dLon)

    (newLon, newLat)
  }


  def haversineDistanceMeters(
    lon1: Double,
    lat1: Double,
    lon2: Double,
    lat2: Double
  ): Double = {

    val dLat =
      deg2rad(lat2 - lat1)

    val dLon =
      deg2rad(lon2 - lon1)

    val a =
      math.sin(dLat / 2) * math.sin(dLat / 2) +
        math.cos(deg2rad(lat1)) *
        math.cos(deg2rad(lat2)) *
        math.sin(dLon / 2) *
        math.sin(dLon / 2)

    val c =
      2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a)
      )

    EarthRadiusMeters * c
  }


  def metersToLatitudeDegrees(meters: Double): Double = {
    meters / 111320.0
  }


  def metersToLongitudeDegrees(latitude: Double, meters: Double): Double = {
    meters / (111320.0 * math.cos(deg2rad(latitude)))
  }
}
