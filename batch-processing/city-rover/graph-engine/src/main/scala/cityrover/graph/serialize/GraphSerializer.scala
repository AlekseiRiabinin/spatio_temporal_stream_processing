package cityrover.graph.serialize

import cityrover.graph.builder.RoadGraphBuilder.{GraphNode, GraphEdge}
import cityrover.graph.spatial.SpatialIndexBuilder.SpatialIndex

import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.io.{OutputFile, PositionOutputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

import scala.collection.JavaConverters._

import java.io.{File, FileOutputStream}


/**
  * GraphSerializer writes:
  *
  *   - nodes.parquet
  *   - edges.parquet
  *   - spatial-index.parquet
  *
  * Parquet is used for:
  *   - Spark processing
  *   - offline analytics
  *   - rover simulator loading
  *
  * Spatial index persistence stores only index metadata.
  * STRtree is rebuilt in memory by consumers.
  */
object GraphSerializer {


  // ---------------------------------------------------------------------------
  // Avro schemas
  // ---------------------------------------------------------------------------

  private val nodeSchema: Schema =
    new Schema.Parser().parse(
      """
      {
        "type": "record",
        "name": "GraphNode",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "lat", "type": "double"},
          {"name": "lon", "type": "double"},
          {
            "name": "outgoingEdges",
            "type": {
              "type": "array",
              "items": "string"
            }
          }
        ]
      }
      """
    )


  private val edgeSchema: Schema =
    new Schema.Parser().parse(
      """
      {
        "type": "record",
        "name": "GraphEdge",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "from", "type": "string"},
          {"name": "to", "type": "string"},
          {
            "name": "geometry",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "Point",
                "fields": [
                  {"name": "lat", "type": "double"},
                  {"name": "lon", "type": "double"}
                ]
              }
            }
          },
          {
            "name": "tags",
            "type": {
              "type": "map",
              "values": "string"
            }
          }
        ]
      }
      """
    )


  private val spatialSchema: Schema =
    new Schema.Parser().parse(
      """
      {
        "type":"record",
        "name":"SpatialEntry",
        "fields":[
          {"name":"edgeId","type":"string"},
          {"name":"minLat","type":"double"},
          {"name":"minLon","type":"double"},
          {"name":"maxLat","type":"double"},
          {"name":"maxLon","type":"double"}
        ]
      }
      """
    )



  // ---------------------------------------------------------------------------
  // Local filesystem OutputFile
  // ---------------------------------------------------------------------------

  private def outputFile(path: String): OutputFile =
    new OutputFile {

      override def create(blockSizeHint: Long): PositionOutputStream = {

        val file = new File(path)

        Option(file.getParentFile)
          .foreach(_.mkdirs())

        val out =
          new FileOutputStream(file)


        new PositionOutputStream {

          private var pos = 0L


          override def getPos: Long =
            pos


          override def write(b: Int): Unit = {
            out.write(b)
            pos += 1
          }


          override def write(
            b: Array[Byte],
            off: Int,
            len: Int
          ): Unit = {

            out.write(b, off, len)
            pos += len
          }


          override def flush(): Unit =
            out.flush()


          override def close(): Unit =
            out.close()
        }
      }


      override def createOrOverwrite(
        blockSizeHint: Long
      ): PositionOutputStream =
        create(blockSizeHint)


      override def supportsBlockSize(): Boolean =
        false


      override def defaultBlockSize(): Long =
        0L
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

    if (!dir.exists())
      dir.mkdirs()

    writeNodes(
      outputDir + "/nodes.parquet",
      nodes
    )

    writeEdges(
      outputDir + "/edges.parquet",
      edges
    )

    writeSpatialIndexData(
      outputDir + "/spatial-index.parquet",
      spatialIndex
    )

    println(
      s"[GraphSerializer] Wrote nodes, edges, and spatial index to $outputDir"
    )
  }


  // ---------------------------------------------------------------------------
  // Nodes
  // ---------------------------------------------------------------------------

  private def writeNodes(
    path: String,
    nodes: Map[String, GraphNode]
  ): Unit = {


    val writer =
      AvroParquetWriter
        .builder[GenericRecord](outputFile(path))
        .withSchema(nodeSchema)
        .withCompressionCodec(
          CompressionCodecName.SNAPPY
        )
        .withWriteMode(
          ParquetFileWriter.Mode.OVERWRITE
        )
        .build()


    nodes.values.foreach { n =>

      val rec =
        new GenericData.Record(nodeSchema)

      rec.put("id", n.id)
      rec.put("lat", n.lat)
      rec.put("lon", n.lon)
      rec.put(
        "outgoingEdges",
        n.outgoingEdges.toArray
      )

      writer.write(rec)
    }


    writer.close()
  }



  // ---------------------------------------------------------------------------
  // Edges
  // ---------------------------------------------------------------------------

  private def writeEdges(
    path: String,
    edges: Map[String, GraphEdge]
  ): Unit = {

    val writer =
      AvroParquetWriter
        .builder[GenericRecord](outputFile(path))
        .withSchema(edgeSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

    edges.values.foreach { e =>

      val rec = new GenericData.Record(edgeSchema)

      rec.put("id", e.id)
      rec.put("from", e.from)
      rec.put("to", e.to)

      val geometrySchema =
        edgeSchema
          .getField("geometry")
          .schema()
          .getElementType

      val geomArray =
        new GenericData.Array[GenericRecord](
          e.geometry.size,
          edgeSchema.getField("geometry").schema()
        )

      e.geometry.foreach { case (lat, lon) =>

        val point = new GenericData.Record(geometrySchema)

        point.put("lat", lat)
        point.put("lon", lon)

        geomArray.add(point)
      }

      rec.put("geometry", geomArray)
      rec.put("tags", e.tags.asJava)

      writer.write(rec)
    }

    writer.close()
  }


  // ---------------------------------------------------------------------------
  // Write spatial-index.parquet
  // ---------------------------------------------------------------------------

  private def writeSpatialIndexData(
    path: String,
    spatialIndex: SpatialIndex
  ): Unit = {

    val spatialSchema =
      new Schema.Parser().parse(
        """
        {
          "type":"record",
          "name":"SpatialEntry",
          "fields":[
            {"name":"edgeId","type":"string"},
            {"name":"minLat","type":"double"},
            {"name":"minLon","type":"double"},
            {"name":"maxLat","type":"double"},
            {"name":"maxLon","type":"double"}
          ]
        }
        """
      )

    val writer =
      AvroParquetWriter
        .builder[GenericRecord](outputFile(path))
        .withSchema(spatialSchema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()

    spatialIndex.edges.foreach { case (edgeId, edge) =>

      val coords =
        edge.geometry.map { case (lat, lon) =>
          new Coordinate(lon, lat)
        }.toArray

      if (coords.length >= 2) {

        val line = 
          new GeometryFactory()
            .createLineString(coords)

        val envelope = line.getEnvelopeInternal

        val rec = new GenericData.Record(spatialSchema)

        rec.put("edgeId", edgeId)
        rec.put("minLat", envelope.getMinY)
        rec.put("minLon", envelope.getMinX)
        rec.put("maxLat", envelope.getMaxY)
        rec.put("maxLon", envelope.getMaxX)

        writer.write(rec)
      }
    }

    writer.close()

    println(
      s"[GraphSerializer] Spatial index written: $path"
    )
  }
}
