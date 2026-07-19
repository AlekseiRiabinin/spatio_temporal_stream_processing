package cityrover.sim.graph

import com.typesafe.config.Config

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.avro.AvroParquetReader

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import java.io.{FileInputStream, ObjectInputStream}


/**
  * GraphService loads the real rover graph produced by graph-engine:
  *   - nodes.parquet
  *   - edges.parquet
  *   - spatial-index.bin (STRtree)
  *
  * Provides:
  *   - random start node
  *   - random route generation
  *   - geometry lookup
  *   - speed limit lookup
  *   - lat/lon interpolation
  */
class GraphService(config: Config) {

  private val random = new Random()

  // ---------------------------------------------------------------------------
  // Graph model (matches graph-engine output)
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
    geometry: Seq[(Double, Double)],   // polyline
    speedLimit: Double                 // m/s
  )

  // ---------------------------------------------------------------------------
  // Load graph-engine output
  // ---------------------------------------------------------------------------

  private val graphDir: String = config.getString("graph.path")

  private val nodes: Map[String, Node] = loadNodes(graphDir + "/nodes.parquet")
  private val edges: Map[String, Edge] = loadEdges(graphDir + "/edges.parquet")
  private val spatialIndex = loadSpatialIndex(graphDir + "/spatial-index.bin")

  println(s"[GraphService] Loaded graph: nodes=${nodes.size}, edges=${edges.size}")

  // ---------------------------------------------------------------------------
  // Load Nodes
  // ---------------------------------------------------------------------------

  private def loadNodes(path: String): Map[String, Node] = {

    val input = HadoopInputFile.fromPath(
      new Path(path),
      new Configuration()
    )
    val reader = AvroParquetReader.builder[GenericRecord](input).build()
    val buffer = mutable.Map[String, Node]()

    try {

      var rec: GenericRecord = reader.read()

      while (rec != null) {

        val id = rec.get("id").toString
        val lat = rec.get("lat").asInstanceOf[Double]
        val lon = rec.get("lon").asInstanceOf[Double]

        val outgoing =
          rec.get("outgoingEdges")
            .asInstanceOf[java.util.Collection[String]]
            .asScala
            .toSeq

        buffer += id -> Node(id, lat, lon, outgoing)

        rec = reader.read()
      }

    } finally {
      reader.close()
    }

    buffer.toMap
  }


  // ---------------------------------------------------------------------------
  // Load Edges
  // ---------------------------------------------------------------------------

  private def loadEdges(path: String): Map[String, Edge] = {

    val input = HadoopInputFile.fromPath(
      new Path(path),
      new Configuration()
    )
    val reader = AvroParquetReader.builder[GenericRecord](input).build()
    val buffer = mutable.Map[String, Edge]()

    try {

      var rec: GenericRecord = reader.read()

      while (rec != null) {

        val id = rec.get("id").toString
        val from = rec.get("from").toString
        val to = rec.get("to").toString

        val geometry =
          rec.get("geometry")
            .asInstanceOf[java.util.Collection[GenericRecord]]
            .asScala
            .map { p =>
              (
                p.get("lat").asInstanceOf[Double],
                p.get("lon").asInstanceOf[Double]
              )
            }
            .toSeq

        val tags =
          rec.get("tags")
            .asInstanceOf[java.util.Map[String, String]]
            .asScala
            .toMap

        val speedLimit =
          tags
            .get("speed_mps")
            .map(_.toDouble)
            .getOrElse(13.9)

        buffer += id -> Edge(id, from, to, geometry, speedLimit)

        rec = reader.read()
      }

    } finally {
      reader.close()
    }

    buffer.toMap
  }


  private def loadSpatialIndex(path: String): AnyRef = {
    val in = new ObjectInputStream(new FileInputStream(path))
    val index = in.readObject()
    in.close()
    index
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
    val route = mutable.ArrayBuffer[String]()

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
