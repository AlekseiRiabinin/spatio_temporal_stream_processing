package cityrover.sim.rover

import cityrover.sim.graph.GraphService
import cityrover.sim.model.RoverState


/**
  * RoverController simulates movement of a rover along a predefined route.
  *
  * Responsibilities:
  *   - advance rover along current edge
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

  private var currentEdgeIndex: Int = 0
  private var positionOnEdge: Double = 0.0

  // Base speed (m/s). You can later add noise or dynamic speed profiles.
  private val baseSpeed: Double = 10.0

  /** Advance rover by dtMillis and return updated RoverState. */
  def step(dtMillis: Long): RoverState = {

    val dtSeconds = dtMillis / 1000.0
    val speed = baseSpeed

    // Advance normalized position
    val edgeLength = 1.0  // normalized (0–1)
    val delta = speed * dtSeconds / 30.0  // 30m edge length assumption (replace with real)
    positionOnEdge += delta

    // If rover reaches end of edge → move to next edge
    if (positionOnEdge >= edgeLength) {
      positionOnEdge = 0.0
      currentEdgeIndex = (currentEdgeIndex + 1) % route.size
    }

    val edgeId = route(currentEdgeIndex)

    // Compute lat/lon from geometry
    val (lat, lon) = graphService.interpolatePosition(edgeId, positionOnEdge)

    // Compute heading from geometry
    val heading = computeHeading(edgeId, positionOnEdge)

    // Build RoverState
    RoverState(
      roverId = roverId,
      edgeId = edgeId,
      positionOnEdge = positionOnEdge,
      speedMps = speed,
      heading = heading,
      lat = lat,
      lon = lon,
      routeId = route.mkString("-"),
      timestamp = System.currentTimeMillis()
    )
  }

  /** Compute heading based on edge geometry. */
  private def computeHeading(edgeId: String, pos: Double): Double = {
    val geom = graphService.getEdge(edgeId).geometry
    if (geom.size < 2) return 0.0

    val idx = (pos * (geom.size - 1)).toInt
    val nextIdx = math.min(idx + 1, geom.size - 1)

    val (lat1, lon1) = geom(idx)
    val (lat2, lon2) = geom(nextIdx)

    val dy = lat2 - lat1
    val dx = lon2 - lon1

    val angle = math.toDegrees(math.atan2(dy, dx))
    if (angle < 0) angle + 360 else angle
  }
}
