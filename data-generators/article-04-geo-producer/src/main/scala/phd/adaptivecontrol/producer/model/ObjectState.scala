package phd.adaptivecontrol.producer.model


final case class ObjectState(
  lon: Double,
  lat: Double,
  speed: Double,
  heading: Double,
  lastTimestamp: Long
) {

  def withPosition(newLon: Double, newLat: Double): ObjectState =
    copy(
      lon = newLon,
      lat = newLat
    )

  def withMotion(newSpeed: Double, newHeading: Double): ObjectState =
    copy(
      speed = newSpeed,
      heading = newHeading
    )

  def withTimestamp(timestamp: Long): ObjectState =
    copy(
      lastTimestamp = timestamp
    )
}
