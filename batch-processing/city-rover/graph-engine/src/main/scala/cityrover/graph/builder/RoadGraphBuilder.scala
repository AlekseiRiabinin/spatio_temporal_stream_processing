package cityrover.graph.builder

import cityrover.graph.loader.OSMLoader.{RawOSMData, RawNode, RawWay}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LineString}

import scala.collection.mutable


/**
  * RoadGraphBuilder transforms RawOSMData (from Postgres or PBF)
  * into a directed road graph.
  *
  * Output:
  *   RoadGraph(
  *     nodes: Map[String, GraphNode],
  *     edges: Map[String, GraphEdge]
  *   )
  *
  * Each OSM/Postgres way becomes one or more directed edges.
  */
object RoadGraphBuilder {

  // ---------------------------------------------------------------------------
  // Internal graph models
  // ---------------------------------------------------------------------------

  case class GraphNode(
    id: String,
    lat: Double,
    lon: Double,
    outgoingEdges: Seq[String]
  )

  case class GraphEdge(
    id: String,
    from: String,
    to: String,
    geometry: Seq[(Double, Double)],
    tags: Map[String, String]
  )

  case class RoadGraph(
    nodes: Map[String, GraphNode],
    edges: Map[String, GraphEdge]
  )

  private val geomFactory = new GeometryFactory()

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  def build(osm: RawOSMData): RoadGraph = {

    val nodes = mutable.Map[String, GraphNode]()
    val edges = mutable.Map[String, GraphEdge]()

    // -------------------------------------------------------------------------
    // Build graph nodes
    // -------------------------------------------------------------------------
    osm.nodes.foreach { case (id, raw) =>
      nodes += id.toString -> GraphNode(
        id = id.toString,
        lat = raw.lat,
        lon = raw.lon,
        outgoingEdges = Seq.empty
      )
    }

    // -------------------------------------------------------------------------
    // Build graph edges
    // -------------------------------------------------------------------------
    osm.ways.foreach { way =>
      val isOneway = way.tags.get("oneway").contains("true") ||
                     way.tags.get("oneway").contains("yes")

      // Forward direction
      buildEdgeFromWay(way, forward = true, nodes, edges)

      // Backward direction (if not oneway)
      if (!isOneway)
        buildEdgeFromWay(way, forward = false, nodes, edges)
    }

    RoadGraph(
      nodes = nodes.toMap,
      edges = edges.toMap
    )
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def buildEdgeFromWay(
    way: RawWay,
    forward: Boolean,
    nodes: mutable.Map[String, GraphNode],
    edges: mutable.Map[String, GraphEdge]
  ): Unit = {

    val nodeSeq = if (forward) way.nodeIds else way.nodeIds.reverse

    if (nodeSeq.size < 2) return

    val fromId = nodeSeq.head.toString
    val toId   = nodeSeq.last.toString

    val geometry =
      nodeSeq.flatMap(id => nodes.get(id.toString).map(n => (n.lat, n.lon)))

    val edgeId = s"${way.id}-${if (forward) "f" else "b"}"

    val edge = GraphEdge(
      id = edgeId,
      from = fromId,
      to = toId,
      geometry = geometry,
      tags = way.tags
    )

    edges += edgeId -> edge

    // Update outgoing edges
    val gn = nodes(fromId)
    nodes.update(fromId, gn.copy(outgoingEdges = gn.outgoingEdges :+ edgeId))
  }
}
