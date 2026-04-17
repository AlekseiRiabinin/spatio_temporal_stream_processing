package phd.spatialmethods.spatial

import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import phd.spatialmethods.model.{GeoEvent, SpatialRegion}


/**
 * SpatialOperations provides core spatial primitives
 * used across the streaming pipeline.
 *
 * NOTE: Uses JTS for geometry parsing, but distance is computed in meters.
 */
object SpatialOperations {

  private val geometryFactory = new GeometryFactory()
  private val wktReader = new WKTReader(geometryFactory)

  private val EarthRadiusMeters = 6371000.0

  /**
   * Parse WKT into Geometry
   */
  def parseWKT(wkt: String): Geometry =
    wktReader.read(wkt)

  /**
   * Convert GeoEvent to Point geometry
   */
  def toPoint(event: GeoEvent): Point =
    geometryFactory.createPoint(new Coordinate(event.lon, event.lat))

  /**
   * Check if event is inside region (Point-in-Polygon)
   */
  def contains(event: GeoEvent, region: SpatialRegion): Boolean = {
    val point = toPoint(event)
    val polygon = parseWKT(region.wkt)
    polygon.contains(point)
  }

  /**
   * Haversine distance between two GeoEvents (meters)
   */
  def distance(e1: GeoEvent, e2: GeoEvent): Double = {
    val lat1 = math.toRadians(e1.lat)
    val lon1 = math.toRadians(e1.lon)
    val lat2 = math.toRadians(e2.lat)
    val lon2 = math.toRadians(e2.lon)

    val dLat = lat2 - lat1
    val dLon = lon2 - lon1

    val a =
      math.sin(dLat / 2) * math.sin(dLat / 2) +
      math.cos(lat1) * math.cos(lat2) *
      math.sin(dLon / 2) * math.sin(dLon / 2)

    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    EarthRadiusMeters * c
  }

  /**
   * Check if two events are within a threshold distance (meters)
   */
  def withinDistance(
    e1: GeoEvent,
    e2: GeoEvent,
    threshold: Double
  ): Boolean =
    distance(e1, e2) <= threshold

  /**
   * Bounding box intersection check between region and event
   */
  def intersects(event: GeoEvent, region: SpatialRegion): Boolean = {
    val point = toPoint(event)
    val geom = parseWKT(region.wkt)
    geom.getEnvelopeInternal.contains(point.getCoordinate)
  }

  /**
   * Compute centroid of region
   */
  def centroid(region: SpatialRegion): Point = {
    val geom = parseWKT(region.wkt)
    geom.getCentroid
  }

  /**
   * Compute bounding box of region
   */
  def boundingBox(region: SpatialRegion): Envelope = {
    val geom = parseWKT(region.wkt)
    geom.getEnvelopeInternal
  }

  /**
   * Spatial join: assign each event to regions
   */
  def spatialJoin(
    events: Seq[GeoEvent],
    regions: Seq[SpatialRegion]
  ): Seq[(GeoEvent, SpatialRegion)] = {

    for {
      e <- events
      r <- regions
      if contains(e, r)
    } yield (e, r)
  }

  /**
   * Haversine distance for latitude difference only (meters)
   * Same longitude, varying latitude.
   */
  def distanceLat(lat1: Double, lat2: Double): Double = {
    val lat1r = math.toRadians(lat1)
    val lat2r = math.toRadians(lat2)
    val dLat = lat2r - lat1r

    val a = math.sin(dLat / 2) * math.sin(dLat / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    EarthRadiusMeters * c
  }

  /**
   * Haversine distance for longitude difference only (meters)
   * Same latitude, varying longitude.
   */
  def distanceLon(lat: Double, lon1: Double, lon2: Double): Double = {
    val latr = math.toRadians(lat)
    val lon1r = math.toRadians(lon1)
    val lon2r = math.toRadians(lon2)
    val dLon = lon2r - lon1r

    val a =
      math.cos(latr) * math.cos(latr) *
      math.sin(dLon / 2) * math.sin(dLon / 2)

    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    EarthRadiusMeters * c
  }
}
