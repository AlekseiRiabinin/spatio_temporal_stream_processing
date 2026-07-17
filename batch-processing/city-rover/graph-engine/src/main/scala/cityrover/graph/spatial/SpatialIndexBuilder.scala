package cityrover.graph.spatial

import cityrover.graph.builder.RoadGraphBuilder.{RoadGraph, GraphEdge}
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, LineString}
import org.locationtech.jts.index.strtree.STRtree


/**
  * SpatialIndexBuilder constructs an R-tree (STRtree) over all edges.
  *
  * Each edge is represented by its bounding box (Envelope) and a LineString.
  *
  * Output:
  *   SpatialIndex(
  *     index: STRtree,
  *     edges: Map[String, GraphEdge]
  *   )
  */
object SpatialIndexBuilder {

  case class SpatialIndex(
    index: STRtree,
    edges: Map[String, GraphEdge]
  )

  private val geomFactory = new GeometryFactory()

  /** Build spatial index from road graph */
  def build(graph: RoadGraph): SpatialIndex = {

    val tree = new STRtree()

    graph.edges.foreach { case (edgeId, edge) =>

      // Convert geometry to JTS LineString
      val coords = edge.geometry.map { case (lat, lon) =>
        new Coordinate(lon, lat) // JTS uses (x=lon, y=lat)
      }.toArray

      if (coords.length >= 2) {
        val line = geomFactory.createLineString(coords)
        val envelope = line.getEnvelopeInternal

        // Insert into R-tree
        tree.insert(envelope, (edgeId, line))
      }
    }

    tree.build()

    SpatialIndex(
      index = tree,
      edges = graph.edges
    )
  }
}
