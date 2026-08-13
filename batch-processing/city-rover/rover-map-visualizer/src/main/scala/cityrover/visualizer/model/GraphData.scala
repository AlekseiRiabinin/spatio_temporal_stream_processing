package cityrover.visualizer.model

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._


/**
  * Represents the full road graph used by:
  *   GET /api/graph
  *
  * The graph consists of:
  *   - nodes: each with id, lat, lon
  *   - edges: each with id, from, to
  *
  * This structure matches the output of your graph-engine
  * and is easy for the frontend to render using Leaflet.
  */
case class GraphNode(
  id: String,
  lat: Double,
  lon: Double
)

object GraphNode {
  implicit val encoder: Encoder[GraphNode] = deriveEncoder[GraphNode]
  implicit val decoder: Decoder[GraphNode] = deriveDecoder[GraphNode]
}

case class GraphEdge(
  id: String,
  from: String,
  to: String
)

object GraphEdge {
  implicit val encoder: Encoder[GraphEdge] = deriveEncoder[GraphEdge]
  implicit val decoder: Decoder[GraphEdge] = deriveDecoder[GraphEdge]
}

case class GraphData(
  nodes: Seq[GraphNode],
  edges: Seq[GraphEdge]
)

object GraphData {
  implicit val encoder: Encoder[GraphData] = deriveEncoder[GraphData]
  implicit val decoder: Decoder[GraphData] = deriveDecoder[GraphData]
}
