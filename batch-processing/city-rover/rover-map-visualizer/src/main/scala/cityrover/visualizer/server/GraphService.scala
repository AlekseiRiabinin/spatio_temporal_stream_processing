package cityrover.visualizer.service

import cityrover.visualizer.model.{GraphData, GraphNode, GraphEdge}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import io.circe.Json
import io.circe.parser._


class GraphService(config: Config) {

  private val log = LoggerFactory.getLogger(getClass)

  private val outputDir: String =
    config.getString("visualizer.outputDir")

  private val graphDir =
    Paths.get(outputDir, "graph")

  private val graphFile =
    graphDir.resolve("graph.json")

  /**
    * Load graph data (nodes + edges) from:
    *
    *   <outputDir>/graph/graph.json
    *
    * Expected structure:
    *
    * {
    *   "nodes": [
    *     { "id": "n1", "lat": 25.20, "lon": 55.27 },
    *     ...
    *   ],
    *   "edges": [
    *     { "id": "e1", "from": "n1", "to": "n2" },
    *     ...
    *   ]
    * }
    */
  def getGraph(): GraphData = {

    if (!Files.exists(graphFile)) {
      log.warn(s"[GraphService] Graph file not found: $graphFile")
      return GraphData(Seq.empty, Seq.empty)
    }

    val raw =
      try Files.readString(graphFile)
      catch {
        case ex: Exception =>
          log.error(s"[GraphService] Failed to read graph file: $graphFile", ex)
          return GraphData(Seq.empty, Seq.empty)
      }

    val json =
      parse(raw) match {
        case Left(err) =>
          log.error(s"[GraphService] Failed to parse graph JSON", err)
          return GraphData(Seq.empty, Seq.empty)
        case Right(value) =>
          value
      }

    val cursor = json.hcursor

    val nodes: Seq[GraphNode] =
      cursor.downField("nodes").as[Seq[GraphNode]].getOrElse {
        log.warn("[GraphService] Missing or invalid 'nodes' array")
        Seq.empty
      }

    val edges: Seq[GraphEdge] =
      cursor.downField("edges").as[Seq[GraphEdge]].getOrElse {
        log.warn("[GraphService] Missing or invalid 'edges' array")
        Seq.empty
      }

    GraphData(nodes, edges)
  }
}
