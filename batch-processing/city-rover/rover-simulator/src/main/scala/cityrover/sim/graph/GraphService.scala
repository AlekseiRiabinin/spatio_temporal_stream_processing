package cityrover.sim.graph

import com.typesafe.config.Config

import org.apache.avro.generic.GenericRecord
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.io.{InputFile, SeekableInputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import java.io.{File, RandomAccessFile}

import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.index.strtree.STRtree


/**
  * GraphService loads the real rover graph produced by graph-engine:
  *   - nodes.parquet
  *   - edges.parquet
  *   - spatial-index.parquet
  *
  * The STRtree spatial index is rebuilt from bounding boxes stored
  * in spatial-index.parquet.
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
  // Local filesystem Parquet reader
  // ---------------------------------------------------------------------------

  private def inputFile(path: String): InputFile = new InputFile {

    override def getLength: Long =
      new File(path).length()

    override def newStream(): SeekableInputStream = {

      val raf = new RandomAccessFile(path, "r")

      new SeekableInputStream {

        override def getPos: Long =
          raf.getFilePointer

        override def seek(newPos: Long): Unit =
          raf.seek(newPos)

        override def read(): Int =
          raf.read()

        override def read(
          b: Array[Byte],
          off: Int,
          len: Int
        ): Int =
          raf.read(b, off, len)

        override def read(buffer: java.nio.ByteBuffer): Int = {
          val remaining = buffer.remaining()
          val bytes = new Array[Byte](remaining)
          val readBytes = raf.read(bytes)

          if (readBytes > 0) {
            buffer.put(bytes, 0, readBytes)
          }

          readBytes
        }

        override def readFully(buffer: java.nio.ByteBuffer): Unit = {

          while (buffer.hasRemaining) {
            val n = read(buffer)

            if (n < 0) throw new java.io.EOFException()
          }
        }

        override def readFully(b: Array[Byte]): Unit =
          raf.readFully(b)

        override def readFully(
          b: Array[Byte],
          off: Int,
          len: Int
        ): Unit =
          raf.readFully(b, off, len)

        override def close(): Unit =
          raf.close()
      }
    }
  }


  // ---------------------------------------------------------------------------
  // Build Spatial Index
  // ---------------------------------------------------------------------------

  private def buildSpatialIndex(path: String): STRtree = {

    println(s"[GraphService] Loading spatial index: $path")

    val tree = new STRtree()

    val reader =
      AvroParquetReader
        .builder[GenericRecord](inputFile(path))
        .build()

    try {

      var rec = reader.read()
      var count = 0

      while (rec != null) {

        val minLon = rec.get("minLon").asInstanceOf[Double]
        val maxLon = rec.get("maxLon").asInstanceOf[Double]
        val minLat = rec.get("minLat").asInstanceOf[Double]
        val maxLat = rec.get("maxLat").asInstanceOf[Double]

        val envelope = new Envelope(minLon, maxLon, minLat, maxLat)

        val edgeId = rec.get("edgeId").toString

        tree.insert(envelope, edgeId)

        count += 1

        rec = reader.read()
      }

      tree.build()

      println(
        s"[GraphService] Spatial index built: $count entries"
      )

    } finally {
      reader.close()
    }

    tree
  }


  // ---------------------------------------------------------------------------
  // Load graph-engine output
  // ---------------------------------------------------------------------------

  private val graphDir: String = {
    val path = config.getString("path")
    println(s"[GraphService] Graph path: $path")
    path
  }

  private val nodes = loadNodes(graphDir + "/nodes.parquet")
  private val edges = loadEdges(graphDir + "/edges.parquet")

  private val spatialIndex =
    buildSpatialIndex(graphDir + "/spatial-index.parquet")

  println(
    s"[GraphService] Loaded graph: nodes=${nodes.size}, edges=${edges.size}"
  )


  // ---------------------------------------------------------------------------
  // Load Nodes
  // ---------------------------------------------------------------------------

  private def loadNodes(path: String): Map[String, Node] = {

    println(s"[GraphService] Loading nodes: $path")

    val reader =
      AvroParquetReader
        .builder[GenericRecord](inputFile(path))
        .build()

    val buffer = mutable.Map[String, Node]()

    try {

      var rec = reader.read()

      while (rec != null) {

        val id = rec.get("id").toString
        val lat = rec.get("lat").asInstanceOf[Double]
        val lon = rec.get("lon").asInstanceOf[Double]

        val outgoing =
          rec.get("outgoingEdges")
            .asInstanceOf[java.util.Collection[_]]
            .asScala
            .map(_.toString)
            .toSeq

        buffer += id -> Node(id, lat, lon, outgoing)

        rec = reader.read()
      }

    } finally {
      reader.close()
    }

    println(s"[GraphService] Nodes loaded: ${buffer.size}")

    buffer.toMap
  }


  // ---------------------------------------------------------------------------
  // Load Edges
  // ---------------------------------------------------------------------------

  private def loadEdges(path: String): Map[String, Edge] = {

    println(s"[GraphService] Loading edges: $path")

    val reader =
      AvroParquetReader
        .builder[GenericRecord](inputFile(path))
        .build()

    val buffer = mutable.Map[String, Edge]()

    try {

      var rec = reader.read()

      while (rec != null) {

        val id = rec.get("id").toString
        val from = rec.get("from").toString
        val to = rec.get("to").toString

        val geometry =
          rec.get("geometry")
            .asInstanceOf[java.util.Collection[_]]
            .asScala
            .map(_.asInstanceOf[GenericRecord])
            .map { p =>
              (
                p.get("lat").asInstanceOf[Double],
                p.get("lon").asInstanceOf[Double]
              )
            }
            .toSeq

        val tags =
          rec.get("tags")
            .asInstanceOf[java.util.Map[_, _]]
            .asScala
            .map { case (k, v) => k.toString -> v.toString }
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

    println(s"[GraphService] Edges loaded: ${buffer.size}")

    buffer.toMap
  }


  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Pick a random start node for a rover. */
  def getRandomStartNode(): String = {

      val candidates =
        nodes
          .values
          .filter(_.outgoingEdges.nonEmpty)
          .map(_.id)
          .toSeq

      candidates(random.nextInt(candidates.size))
  }

  /** Generate a random route of N edges starting from a given node. */
  def getRandomRoute(startNode: String, length: Int): Seq[String] = {

      var currentNode = startNode
      val route = mutable.ArrayBuffer[String]()

      var i = 0

      while (i < length) {

        val outgoing = nodes(currentNode).outgoingEdges

        if (outgoing.isEmpty) {
          return route.toSeq
        }

        val edgeId = outgoing(random.nextInt(outgoing.size))

        route += edgeId

        currentNode = edges(edgeId).to

        i += 1
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

  def queryNearbyEdges(
    lat: Double,
    lon: Double,
    radius: Double
  ): Seq[String] = {

    val env = new Envelope(
      lon - radius,
      lon + radius,
      lat - radius,
      lat + radius
    )

    spatialIndex
      .query(env)
      .asInstanceOf[java.util.List[String]]
      .asScala
      .toSeq
  }
}
