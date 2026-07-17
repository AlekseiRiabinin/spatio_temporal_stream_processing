package cityrover.graph.serialize

import cityrover.graph.builder.RoadGraphBuilder.{GraphNode, GraphEdge}
import cityrover.graph.spatial.SpatialIndexBuilder.SpatialIndex

import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.collection.JavaConverters._
import java.io.{File, FileOutputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.parquet.io.{OutputFile, PositionOutputStream}


/**
  * GraphSerializer writes:
  *   - nodes.parquet
  *   - edges.parquet
  *   - spatial-index.bin
  *
  * Parquet is used for Spark jobs.
  * Binary serialization is used for STRtree spatial index.
  */
object GraphSerializer {

  // ---------------------------------------------------------------------------
  // Avro schemas for Parquet
  // ---------------------------------------------------------------------------

  private val nodeSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |  "type": "record",
      |  "name": "GraphNode",
      |  "fields": [
      |    {"name": "id", "type": "string"},
      |    {"name": "lat", "type": "double"},
      |    {"name": "lon", "type": "double"},
      |    {"name": "outgoingEdges", "type": {"type": "array", "items": "string"}}
      |  ]
      |}
      |""".stripMargin)

  private val edgeSchema: Schema = new Schema.Parser().parse(
    """
      |{
      |  "type": "record",
      |  "name": "GraphEdge",
      |  "fields": [
      |    {"name": "id", "type": "string"},
      |    {"name": "from", "type": "string"},
      |    {"name": "to", "type": "string"},
      |    {"name": "geometry", "type": {"type": "array", "items": {
      |        "type": "record",
      |        "name": "Point",
      |        "fields": [
      |          {"name": "lat", "type": "double"},
      |          {"name": "lon", "type": "double"}
      |        ]
      |    }}},
      |    {"name": "tags", "type": {"type": "map", "values": "string"}}
      |  ]
      |}
      |""".stripMargin)

  // ---------------------------------------------------------------------------
  // OutputFile wrapper (modern Parquet API)
  // ---------------------------------------------------------------------------

  private def outputFile(path: Path): OutputFile = new OutputFile {
    private val fs: FileSystem = path.getFileSystem(new Configuration())

    override def create(blockSizeHint: Long): PositionOutputStream = {
      val out: FSDataOutputStream = fs.create(path, true)
      new PositionOutputStream {
        override def getPos: Long = out.getPos
        override def write(b: Int): Unit = out.write(b)
        override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)
        override def flush(): Unit = out.flush()
        override def close(): Unit = out.close()
      }
    }

    override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
      create(blockSizeHint)

    override def supportsBlockSize(): Boolean = false
    override def defaultBlockSize(): Long = 0L
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  def write(
    outputDir: String,
    nodes: Map[String, GraphNode],
    edges: Map[String, GraphEdge],
    spatialIndex: SpatialIndex
  ): Unit = {

    val dir = new File(outputDir)
    if (!dir.exists()) dir.mkdirs()

    writeNodes(outputDir + "/nodes.parquet", nodes)
    writeEdges(outputDir + "/edges.parquet", edges)
    writeSpatialIndex(outputDir + "/spatial-index.bin", spatialIndex)

    println(s"[GraphSerializer] Wrote nodes, edges, and spatial index to $outputDir")
  }

  // ---------------------------------------------------------------------------
  // Write nodes.parquet (modern API)
  // ---------------------------------------------------------------------------

  private def writeNodes(path: String, nodes: Map[String, GraphNode]): Unit = {

    val writer: ParquetWriter[GenericRecord] =
      AvroParquetWriter
        .builder(outputFile(new Path(path)))
        .withSchema(nodeSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

    nodes.values.foreach { n =>
      val rec = new GenericData.Record(nodeSchema)
      rec.put("id", n.id)
      rec.put("lat", n.lat)
      rec.put("lon", n.lon)
      rec.put("outgoingEdges", n.outgoingEdges.toArray)
      writer.write(rec)
    }

    writer.close()
  }

  // ---------------------------------------------------------------------------
  // Write edges.parquet (modern API)
  // ---------------------------------------------------------------------------

  private def writeEdges(path: String, edges: Map[String, GraphEdge]): Unit = {

    val writer: ParquetWriter[GenericRecord] =
      AvroParquetWriter
        .builder(outputFile(new Path(path)))
        .withSchema(edgeSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

    edges.values.foreach { e =>
      val rec = new GenericData.Record(edgeSchema)
      rec.put("id", e.id)
      rec.put("from", e.from)
      rec.put("to", e.to)

      val geomArray = new GenericData.Array[GenericRecord](
        e.geometry.size,
        edgeSchema.getField("geometry").schema()
      )

      e.geometry.foreach { case (lat, lon) =>
        val pointSchema = edgeSchema.getField("geometry").schema().getElementType
        val pointRec = new GenericData.Record(pointSchema)
        pointRec.put("lat", lat)
        pointRec.put("lon", lon)
        geomArray.add(pointRec)
      }

      rec.put("geometry", geomArray)
      rec.put("tags", e.tags.asJava)

      writer.write(rec)
    }

    writer.close()
  }

  // ---------------------------------------------------------------------------
  // Write spatial-index.bin
  // ---------------------------------------------------------------------------

  private def writeSpatialIndex(path: String, spatialIndex: SpatialIndex): Unit = {
    val out = new ObjectOutputStream(new FileOutputStream(path))
    out.writeObject(spatialIndex.index)
    out.close()
  }
}
