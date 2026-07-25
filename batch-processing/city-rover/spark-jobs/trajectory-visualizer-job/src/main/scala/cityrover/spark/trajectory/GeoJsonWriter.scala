package cityrover.spark.trajectory

import io.circe.syntax._
import io.circe.{Json, Encoder}
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets


object GeoJsonWriter {

  /** Build a GeoJSON LineString for a rover trajectory */
  def toLineString(
    roverId: String,
    coords: Seq[Seq[Double]],
    properties: Map[String, Json] = Map.empty
  ): Json = {

    val baseProps = Map("roverId" -> Json.fromString(roverId)) ++ properties

    Json.obj(
      "type" -> Json.fromString("Feature"),
      "geometry" -> Json.obj(
        "type" -> Json.fromString("LineString"),
        "coordinates" -> coords.asJson
      ),
      "properties" -> Json.obj(baseProps.toSeq: _*)
    )
  }

  /** Build a GeoJSON Point (useful for debugging or future live map) */
  def toPoint(
    roverId: String,
    lat: Double,
    lon: Double,
    properties: Map[String, Json] = Map.empty
  ): Json = {

    val baseProps = Map("roverId" -> Json.fromString(roverId)) ++ properties

    Json.obj(
      "type" -> Json.fromString("Feature"),
      "geometry" -> Json.obj(
        "type" -> Json.fromString("Point"),
        "coordinates" -> Json.arr(Json.fromDoubleOrNull(lon), Json.fromDoubleOrNull(lat))
      ),
      "properties" -> Json.obj(baseProps.toSeq: _*)
    )
  }

  /** Write GeoJSON to a file */
  def writeToFile(path: String, json: Json): Unit = {
    val p = Paths.get(path)
    val parent = p.getParent

    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent)
    }

    Files.write(p, json.spaces2.getBytes(StandardCharsets.UTF_8))
  }
}
