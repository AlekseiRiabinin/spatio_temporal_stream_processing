package cityrover.visualizer.service

import cityrover.visualizer.model.Rover
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._


class RoverService(config: Config) {

  private val log = LoggerFactory.getLogger(getClass)

  // Directory where trajectory-visualizer-job writes GeoJSON files
  private val outputDir: String =
    config.getString("visualizer.outputDir")

  private val geojsonDir =
    Paths.get(outputDir, "geojson")

  /**
    * List all rovers by scanning the GeoJSON directory.
    *
    * Each rover corresponds to one file:
    *   <roverId>.json
    */
  def listRovers(): Seq[Rover] = {
    if (!Files.exists(geojsonDir)) {
      log.warn(s"[RoverService] GeoJSON directory does not exist: $geojsonDir")
      return Seq.empty
    }

    val files =
      Files.list(geojsonDir).iterator().asScala.toSeq

    val roverIds =
      files
        .filter(_.getFileName.toString.endsWith(".json"))
        .map(_.getFileName.toString.replace(".json", ""))
        .sorted

    roverIds.map(Rover.apply)
  }
}
