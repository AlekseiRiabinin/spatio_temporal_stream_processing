package phd.spatialmethods.model


/**
 * Trajectory represents an ordered sequence of GeoEvents
 * belonging to the same moving object.
 *
 * @param objectId Identifier of the moving object (rover, drone, etc.)
 * @param events   Time-ordered sequence of events
 */
case class Trajectory(
  objectId: String,
  events: Seq[GeoEvent]
) {

  /**
   * Ensures events are sorted by event-time
   */
  lazy val sortedEvents: Seq[GeoEvent] =
    events.sortBy(_.timestamp)

  /**
   * Start time of trajectory (epoch millis)
   */
  def startTime: Option[Long] =
    sortedEvents.headOption.map(_.timestamp)

  /**
   * End time of trajectory (epoch millis)
   */
  def endTime: Option[Long] =
    sortedEvents.lastOption.map(_.timestamp)

  /**
   * Duration in milliseconds
   */
  def durationMillis: Long =
    (for {
      start <- startTime
      end   <- endTime
    } yield end - start).getOrElse(0L)

  /**
   * Number of points in trajectory
   */
  def size: Int = events.size

  /**
   * Extract only valid events
   */
  def validEvents: Seq[GeoEvent] =
    events.filter(_.isValid)

  /**
   * Compute total traveled distance (approximate, in meters)
   * Uses Haversine formula
   */
  def totalDistanceMeters: Double = {
    val pts = sortedEvents
    if (pts.size < 2) 0.0
    else {
      pts.sliding(2).map {
        case Seq(a, b) => haversine(a.lat, a.lon, b.lat, b.lon)
      }.sum
    }
  }

  /**
   * Average speed (m/s) based on distance and duration
   */
  def averageSpeed: Double = {
    val durationSec = durationMillis / 1000.0
    if (durationSec == 0) 0.0
    else totalDistanceMeters / durationSec
  }

  /**
   * Bounding box of trajectory: (minLon, minLat, maxLon, maxLat)
   */
  def boundingBox: Option[(Double, Double, Double, Double)] = {
    if (events.isEmpty) None
    else {
      val lons = events.map(_.lon)
      val lats = events.map(_.lat)
      Some((lons.min, lats.min, lons.max, lats.max))
    }
  }

  /**
   * Add new event (used in streaming updates)
   */
  def addEvent(event: GeoEvent): Trajectory =
    copy(events = events :+ event)

  /**
   * Merge two trajectories (same object)
   */
  def merge(other: Trajectory): Trajectory = {
    require(objectId == other.objectId, "Cannot merge trajectories of different objects")
    copy(events = this.events ++ other.events)
  }

  /**
   * Haversine distance between two geo points (meters)
   */
  private def haversine(
    lat1: Double, lon1: Double,
    lat2: Double, lon2: Double
  ): Double = {
    val R = 6371000.0
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)

    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(math.toRadians(lat1)) *
      math.cos(math.toRadians(lat2)) *
      math.sin(dLon / 2) * math.sin(dLon / 2)

    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R * c
  }

  override def toString: String =
    s"Trajectory(objectId=$objectId, points=${events.size})"
}
