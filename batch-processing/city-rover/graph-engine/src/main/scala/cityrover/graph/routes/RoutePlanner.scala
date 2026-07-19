package cityrover.graph.routes

import cityrover.graph.builder.RoadGraphBuilder.{RoadGraph, GraphEdge}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import scala.collection.mutable
import scala.util.Random


/**
  * RoutePlanner provides:
  *   - Dijkstra shortest path (time-weighted)
  *   - Random route generation
  *   - Adjacency list construction
  *
  * Edge weight = travel time = length_meters / speed_mps
  */
object RoutePlanner {

  private val geomFactory = new GeometryFactory()
  private val rnd = new Random()

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Build adjacency list: nodeId -> Seq[(neighborNodeId, edgeId, weight)] */
  def buildAdjacency(graph: RoadGraph): Map[String, Seq[(String, String, Double)]] = {
    val adj = mutable.Map[String, mutable.ArrayBuffer[(String, String, Double)]]()

    graph.edges.values.foreach { edge =>
      val from = edge.from
      val to   = edge.to
      val weight = computeEdgeWeight(edge)

      val list = adj.getOrElseUpdate(from, mutable.ArrayBuffer())
      list += ((to, edge.id, weight))
    }

    adj.mapValues(_.toSeq).toMap
  }

  /** Dijkstra shortest path: returns list of edgeIds */
  def shortestPath(
    graph: RoadGraph,
    startNode: String,
    endNode: String
  ): Seq[String] = {

    val adj = buildAdjacency(graph)

    val dist = mutable.Map[String, Double]().withDefaultValue(Double.PositiveInfinity)
    val prev = mutable.Map[String, String]()
    val prevEdge = mutable.Map[String, String]()

    val pq = mutable.PriorityQueue[(String, Double)]()(Ordering.by(-_._2))

    dist(startNode) = 0.0
    pq.enqueue((startNode, 0.0))

    while (pq.nonEmpty) {
      val (node, _) = pq.dequeue()

      if (node == endNode) {
        return reconstructPath(prev.toMap, prevEdge.toMap, startNode, endNode)
      }

      adj.getOrElse(node, Seq.empty).foreach { case (nbr, edgeId, weight) =>
        val alt = dist(node) + weight
        if (alt < dist(nbr)) {
          dist(nbr) = alt
          prev(nbr) = node
          prevEdge(nbr) = edgeId
          pq.enqueue((nbr, alt))
        }
      }
    }

    Seq.empty // no path found
  }

  /** Generate a random route of N edges */
  def randomRoute(graph: RoadGraph, startNode: String, length: Int): Seq[String] = {
    val adj = buildAdjacency(graph)

    var current = startNode
    val route = mutable.ArrayBuffer[String]()

    for (_ <- 1 to length) {
      val outgoing = adj.getOrElse(current, Seq.empty)
      if (outgoing.isEmpty) return route.toSeq

      val (_, edgeId, _) = outgoing(rnd.nextInt(outgoing.size))
      route += edgeId

      current = graph.edges(edgeId).to
    }

    route.toSeq
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Compute edge weight = length_meters / speed_mps */
  private def computeEdgeWeight(edge: GraphEdge): Double = {

    val coords = edge.geometry.map { case (lat, lon) =>
      new Coordinate(lon, lat)
    }.toArray

    if (coords.length < 2) return Double.PositiveInfinity

    val line = geomFactory.createLineString(coords)

    // Approximate conversion: 1 degree ≈ 111 km
    val lengthMeters = line.getLength * 111000

    // Speed selection:
    // - Postgres mode: tags("speed_mps") may exist
    // - PBF mode: fallback to 13.9 m/s (50 km/h)
    val speed =
      edge.tags.get("speed_mps")
        .flatMap(s => scala.util.Try(s.toDouble).toOption)
        .getOrElse(13.9)

    lengthMeters / speed
  }

  /** Reconstruct path from prev + prevEdge maps */
  private def reconstructPath(
    prev: Map[String, String],
    prevEdge: Map[String, String],
    start: String,
    end: String
  ): Seq[String] = {

    val edges = mutable.ArrayBuffer[String]()
    var node = end

    while (node != start && prev.contains(node)) {
      val edgeId = prevEdge(node)
      edges += edgeId
      node = prev(node)
    }

    edges.reverse.toSeq
  }
}
