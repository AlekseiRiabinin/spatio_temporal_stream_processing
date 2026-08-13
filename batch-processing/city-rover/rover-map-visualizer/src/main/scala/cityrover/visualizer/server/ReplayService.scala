package cityrover.visualizer.service

import cityrover.visualizer.model.{RoverPosition, RoverReplay}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import io.circe.{Json, Decoder}
import io.circe.parser._


class ReplayService(config: Config) {

  private val log = LoggerFactory.getLogger(getClass)

  private val outputDir: String =
    config.getString("visualizer.outputDir")

  private val geojsonDir =
    Paths.get(outputDir, "geojson")

  /**
    * Load replay positions for a given rover.
    *
    * The Spark job produces GeoJSON with:
    *
    * {
    *   "type": "Feature",
    *   "geometry": { ... },
    *   "properties": {
    *     "roverId": "rover-1",
    *     "positions": [ { ts, lat, lon, speed, heading, ... }, ... ]
    *   }
    * }
    */
  def getReplay(roverId: String): RoverReplay = {
    val path = geojsonDir.resolve(s"$roverId.json")

    if (!Files.exists(path)) {
      log.warn(s"[ReplayService] GeoJSON file not found: $path")
      return RoverReplay(roverId, Seq.empty)
    }

    val raw =
      try Files.readString(path)
      catch {
        case ex: Exception =>
          log.error(s"[ReplayService] Failed to read file: $path", ex)
          return RoverReplay(roverId, Seq.empty)
      }

    val json =
      parse(raw) match {
        case Left(err) =>
          log.error(s"[ReplayService] Failed to parse GeoJSON for $roverId", err)
          return RoverReplay(roverId, Seq.empty)
        case Right(value) =>
          value
      }

    // Extract positions array from GeoJSON properties
    val positionsJsonOpt =
      json.hcursor.downField("properties").downField("positions").focus

    val positions: Seq[RoverPosition] =
      positionsJsonOpt match {
        case Some(arr) =>
          arr.asArray match {
            case Some(items) =>
              items.flatMap(_.as[RoverPosition].toOption)
            case None =>
              log.warn(s"[ReplayService] 'positions' is not an array for rover $roverId")
              Seq.empty
          }
        case None =>
          log.warn(s"[ReplayService] No 'positions' field found for rover $roverId")
          Seq.empty
      }

    RoverReplay(roverId, positions)
  }
}
