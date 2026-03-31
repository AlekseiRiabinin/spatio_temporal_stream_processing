package phd.spatialmethods.spatial

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}


/**
 * Utility object for Point-in-Polygon checks.
 * Uses JTS for robust geometric computations.
 */
object PointInPolygon {

  private val geometryFactory = new GeometryFactory()

  /**
   * Checks whether a point (lat, lon) is inside a polygon defined by coordinates.
   *
   * @param lat Latitude of the point
   * @param lon Longitude of the point
   * @param polygonCoords Sequence of (lat, lon) representing polygon vertices (must be closed or will be closed automatically)
   * @return true if point is inside polygon
   */
  def contains(
      lat: Double,
      lon: Double,
      polygonCoords: Seq[(Double, Double)]
  ): Boolean = {

    val point: Point = geometryFactory.createPoint(new Coordinate(lon, lat))

    val coordinates: Array[Coordinate] =
      closePolygon(polygonCoords)
        .map { case (lat, lon) => new Coordinate(lon, lat) }
        .toArray

    val polygon: Polygon = geometryFactory.createPolygon(coordinates)

    polygon.contains(point)
  }

  /**
   * Ensures the polygon is closed (first point == last point)
   */
  private def closePolygon(coords: Seq[(Double, Double)]): Seq[(Double, Double)] = {
    if (coords.head == coords.last) coords
    else coords :+ coords.head
  }
}
