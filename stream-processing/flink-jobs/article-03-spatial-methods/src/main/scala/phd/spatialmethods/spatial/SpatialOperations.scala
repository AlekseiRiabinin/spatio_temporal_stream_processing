package phd.spatialmethods.spatial

import phd.spatialmethods.model.{GeoEvent, SpatialRegion}

import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader


/**
 * SpatialOperations provides core spatial primitives
 * used across the streaming pipeline.
 *
 * NOTE: Uses JTS (Java Topology Suite) for geometry processing.
 */
object SpatialOperations {

  private val geometryFactory = new GeometryFactory()
  private val wktReader = new WKTReader(geometryFactory)

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
   * Distance between two GeoEvents (meters)
   * Uses JTS Euclidean distance (approximate)
   */
  def distance(e1: GeoEvent, e2: GeoEvent): Double = {
    val p1 = toPoint(e1)
    val p2 = toPoint(e2)
    p1.distance(p2)
  }

  /**
   * Check if two events are within a threshold distance
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

}
