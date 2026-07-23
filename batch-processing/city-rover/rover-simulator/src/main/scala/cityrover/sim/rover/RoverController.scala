package cityrover.sim.rover

import cityrover.sim.graph.GraphService
import cityrover.sim.model.RoverState
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}


/**
  * RoverController simulates movement of a rover along a predefined route.
  *
  * Responsibilities:
  *   - advance rover along current edge using real speed limits
  *   - compute real edge length from geometry
  *   - handle transitions between edges
  *   - compute heading
  *   - compute lat/lon using GraphService interpolation
  *   - produce RoverState for each tick
  */
class RoverController(
  val roverId: String,
  val route: Seq[String],
  val graphService: GraphService
) {

  private val geomFactory = new GeometryFactory()

  private var currentEdgeIndex: Int = 0
  private var positionOnEdge: Double = 0.0
  private var traveledMeters: Double = 0.0


  /**
    * Advance rover by dtMillis and return updated RoverState.
    */
  def step(dtMillis: Long): RoverState = {

    if (route.isEmpty) {
      throw new IllegalStateException(
        s"[$roverId] Cannot move: empty route"
      )
    }

    var edgeId = route(currentEdgeIndex)
    var edge = graphService.getEdge(edgeId)

    val dtSeconds = dtMillis / 1000.0
    val speed = edge.speedLimit
    val edgeLengthMeters = computeEdgeLength(edge.geometry)


    // Avoid division by zero for invalid geometries
    if (edgeLengthMeters > 0) {

      traveledMeters += speed * dtSeconds

      positionOnEdge = traveledMeters / edgeLengthMeters

    } else {
      positionOnEdge = 1.0
    }

    // Move to next edge
    if (positionOnEdge >= 1.0) {

      currentEdgeIndex = (currentEdgeIndex + 1) % route.size

      positionOnEdge = 0.0
      traveledMeters = 0.0

      // Update current edge after transition
      edgeId = route(currentEdgeIndex)
      edge = graphService.getEdge(edgeId)
    }

    val (lat, lon) =
      graphService.interpolatePosition(
        edgeId,
        positionOnEdge
      )

    val heading = computeHeading(edgeId, positionOnEdge)

    RoverState(
      roverId = roverId,
      edgeId = edgeId,
      positionOnEdge = positionOnEdge,
      speedMps = edge.speedLimit,
      heading = heading,
      lat = lat,
      lon = lon,
      routeId = route.mkString("-"),
      timestamp = System.currentTimeMillis()
    )
  }


  /**
    * Compute real edge length (meters) from geometry.
    */
  private def computeEdgeLength(geometry: Seq[(Double, Double)]): Double = {

    if (geometry.size < 2) return 0.0

    val coords =
      geometry.map { case (lat, lon) => new Coordinate(lon, lat) }.toArray

    val line = geomFactory.createLineString(coords)

    // JTS returns degrees, approximate conversion to meters
    line.getLength * 111000.0
  }


  /**
    * Compute heading based on geometry.
    */
  private def computeHeading(edgeId: String, pos: Double): Double = {

    val geom = graphService.getEdge(edgeId).geometry

    if (geom.size < 2) return 0.0

    val idx =
      math.min(
        (pos * (geom.size - 1)).toInt,
        geom.size - 2
      )

    val nextIdx = idx + 1
    val (lat1, lon1) = geom(idx)
    val (lat2, lon2) = geom(nextIdx)
    val dy = lat2 - lat1
    val dx = lon2 - lon1

    val angle = math.toDegrees(math.atan2(dy, dx))

    if (angle < 0) angle + 360
    else angle
  }
}
