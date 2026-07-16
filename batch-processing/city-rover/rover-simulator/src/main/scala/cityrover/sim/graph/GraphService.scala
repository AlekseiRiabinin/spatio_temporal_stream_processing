package cityrover.sim.graph

import com.typesafe.config.Config
import scala.util.Random


/**
  * GraphService provides access to the road graph produced by graph-engine.
  * In the simulator, it is used for:
  *   - selecting start nodes
  *   - generating random routes
  *   - retrieving geometry for lat/lon interpolation
  *   - retrieving speed limits
  *
  * NOTE: This is a simplified version. Replace the dummy loaders with your
  *       actual graph-engine outputs (Parquet, JSON, or binary format).
  */
class GraphService(config: Config) {

  private val random = new Random()

  // ---------------------------------------------------------------------------
  // Graph model (simplified)
  // ---------------------------------------------------------------------------

  case class Node(
    id: String,
    lat: Double,
    lon: Double,
    outgoingEdges: Seq[String]
  )

  case class Edge(
    id: String,
    from: String,
    to: String,
    geometry: Seq[(Double, Double)],   // polyline: list of (lat, lon)
    speedLimit: Double                 // m/s
  )

  // ---------------------------------------------------------------------------
  // Graph storage (dummy loaders for now)
  // ---------------------------------------------------------------------------

  private val nodes: Map[String, Node] = loadNodes()
  private val edges: Map[String, Edge] = loadEdges()

  // ---------------------------------------------------------------------------
  // Loaders (replace with real graph-engine output)
  // ---------------------------------------------------------------------------

  private def loadNodes(): Map[String, Node] = {
    // TODO: Replace with real loader
    // Example: read Parquet or JSON from config.getString("path")
    Map(
      "n1" -> Node("n1", 59.93, 30.31, Seq("e1")),
      "n2" -> Node("n2", 59.94, 30.32, Seq("e2")),
      "n3" -> Node("n3", 59.95, 30.33, Seq("e3"))
    )
  }

  private def loadEdges(): Map[String, Edge] = {
    // TODO: Replace with real loader
    Map(
      "e1" -> Edge("e1", "n1", "n2",
        geometry = Seq((59.93, 30.31), (59.94, 30.32)),
        speedLimit = 15.0
      ),
      "e2" -> Edge("e2", "n2", "n3",
        geometry = Seq((59.94, 30.32), (59.95, 30.33)),
        speedLimit = 18.0
      ),
      "e3" -> Edge("e3", "n3", "n1",
        geometry = Seq((59.95, 30.33), (59.93, 30.31)),
        speedLimit = 12.0
      )
    )
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Pick a random start node for a rover. */
  def getRandomStartNode(): String = {
    val keys = nodes.keys.toSeq
    keys(random.nextInt(keys.size))
  }

  /** Generate a random route of N edges starting from a given node. */
  def getRandomRoute(startNode: String, length: Int): Seq[String] = {
    var currentNode = startNode
    val route = scala.collection.mutable.ArrayBuffer[String]()

    for (_ <- 1 to length) {
      val outgoing = nodes(currentNode).outgoingEdges
      if (outgoing.isEmpty) return route.toSeq

      val edgeId = outgoing(random.nextInt(outgoing.size))
      route += edgeId

      currentNode = edges(edgeId).to
    }

    route.toSeq
  }

  /** Retrieve edge metadata. */
  def getEdge(edgeId: String): Edge = edges(edgeId)

  /** Retrieve speed limit for an edge. */
  def getSpeedLimit(edgeId: String): Double = edges(edgeId).speedLimit

  /** Interpolate lat/lon along an edge based on normalized position (0.0–1.0). */
  def interpolatePosition(edgeId: String, pos: Double): (Double, Double) = {
    val geom = edges(edgeId).geometry
    if (geom.size == 1) return geom.head

    val idx = (pos * (geom.size - 1)).toInt
    val nextIdx = math.min(idx + 1, geom.size - 1)

    val (lat1, lon1) = geom(idx)
    val (lat2, lon2) = geom(nextIdx)

    val t = pos - idx.toDouble / (geom.size - 1)

    val lat = lat1 + (lat2 - lat1) * t
    val lon = lon1 + (lon2 - lon1) * t

    (lat, lon)
  }
}
