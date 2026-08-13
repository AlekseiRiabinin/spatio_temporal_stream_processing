package cityrover.visualizer.service

import cityrover.visualizer.model.RoverTrajectory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import io.circe.Json
import io.circe.parser._


class TrajectoryService(config: Config) {

  private val log = LoggerFactory.getLogger(getClass)

  private val outputDir: String =
    config.getString("visualizer.outputDir")

  private val geojsonDir =
    Paths.get(outputDir, "geojson")

  /**
    * Load the GeoJSON trajectory for a given rover.
    *
    * The Spark job produces files:
    *   <outputDir>/geojson/<roverId>.json
    */
  def getTrajectory(roverId: String): RoverTrajectory = {
    val path = geojsonDir.resolve(s"$roverId.json")

    if (!Files.exists(path)) {
      log.warn(s"[TrajectoryService] GeoJSON file not found: $path")
      return RoverTrajectory(roverId, Json.obj())
    }

    val raw =
      try Files.readString(path)
      catch {
        case ex: Exception =>
          log.error(s"[TrajectoryService] Failed to read file: $path", ex)
          return RoverTrajectory(roverId, Json.obj())
      }

    val json =
      parse(raw) match {
        case Left(err) =>
          log.error(s"[TrajectoryService] Failed to parse GeoJSON for $roverId", err)
          Json.obj()
        case Right(value) =>
          value
      }

    RoverTrajectory(roverId, json)
  }
}
