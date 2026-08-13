package cityrover.spark.trajectory

import io.circe.Json
import io.circe.syntax._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}


object GeoJsonWriter {

  /** Build a GeoJSON LineString for a rover trajectory. */
  def toLineString(
    roverId: String,
    coords: Seq[Seq[Double]],
    properties: Map[String, Json] = Map.empty
  ): Json = {

    val baseProps =
      Map("roverId" -> Json.fromString(roverId)) ++ properties

    Json.obj(
      "type" -> Json.fromString("Feature"),
      "geometry" -> Json.obj(
        "type" -> Json.fromString("LineString"),
        "coordinates" -> coords.asJson
      ),
      "properties" -> Json.obj(baseProps.toSeq: _*)
    )
  }

  /**
   * Build a GeoJSON FeatureCollection containing timestamped
   * rover positions for offline trajectory replay.
   *
   * Each position is represented as a GeoJSON Point with
   * telemetry information stored in the properties.
   */
  def toPositionFeatureCollection(
    roverId: String,
    positions: Seq[Map[String, Json]],
    properties: Map[String, Json] = Map.empty
  ): Json = {

    val features = positions.map { position =>

      val lat = position
        .get("lat")
        .flatMap(_.asNumber)
        .map(_.toDouble)
        .getOrElse(0.0)

      val lon = position
        .get("lon")
        .flatMap(_.asNumber)
        .map(_.toDouble)
        .getOrElse(0.0)

      Json.obj(
        "type" -> Json.fromString("Feature"),
        "geometry" -> Json.obj(
          "type" -> Json.fromString("Point"),
          "coordinates" -> Json.arr(
            Json.fromDoubleOrNull(lon),
            Json.fromDoubleOrNull(lat)
          )
        ),
        "properties" ->
          Json.obj(
            (
              Map("roverId" -> Json.fromString(roverId)) ++
                properties ++
                position
            ).toSeq: _*
          )
      )
    }

    Json.obj(
      "type" -> Json.fromString("FeatureCollection"),
      "features" -> features.asJson
    )
  }

  /** Build a GeoJSON Point. */
  def toPoint(
    roverId: String,
    lat: Double,
    lon: Double,
    properties: Map[String, Json] = Map.empty
  ): Json = {

    val baseProps =
      Map("roverId" -> Json.fromString(roverId)) ++ properties

    Json.obj(
      "type" -> Json.fromString("Feature"),
      "geometry" -> Json.obj(
        "type" -> Json.fromString("Point"),
        "coordinates" -> Json.arr(
          Json.fromDoubleOrNull(lon),
          Json.fromDoubleOrNull(lat)
        )
      ),
      "properties" -> Json.obj(baseProps.toSeq: _*)
    )
  }

  /** Write GeoJSON to a file. */
  def writeToFile(
    path: String,
    json: Json
  ): Unit = {

    val p = Paths.get(path)
    val parent = p.getParent

    if (parent != null && !Files.exists(parent)) {
      Files.createDirectories(parent)
    }

    Files.write(
      p,
      json.spaces2.getBytes(StandardCharsets.UTF_8)
    )
  }
}
