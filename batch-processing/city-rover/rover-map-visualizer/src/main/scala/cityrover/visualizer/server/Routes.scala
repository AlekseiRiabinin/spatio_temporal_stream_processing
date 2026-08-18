package cityrover.visualizer.server

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}

import cityrover.visualizer.service._
import com.typesafe.config.Config

import io.circe.syntax._
import io.circe.generic.auto._

import java.nio.file.{Paths, Files}


class Routes(config: Config) {

  private val roverService      = new RoverService(config)
  private val trajectoryService = new TrajectoryService(config)
  private val replayService     = new ReplayService(config)
  private val graphService      = new GraphService(config)

  // "web" from application.conf
  private val webRoot: String = config.getString("visualizer.webRoot")

  // Resolve absolute path inside container
  private val webDirPath = Paths.get("/opt/cityrover", webRoot).toAbsolutePath
  private val webDir = webDirPath.toString

  // Log for debugging
  println(s"[Routes] Serving static files from: $webDir")
  println(s"[Routes] Exists: ${Files.exists(webDirPath)}")

  private def jsonEntity(json: io.circe.Json): HttpEntity.Strict =
    HttpEntity(ContentTypes.`application/json`, json.noSpaces)

  val routes: Route =
    concat(

      // --------------------------------------------------------
      // Static web assets
      // --------------------------------------------------------
      pathPrefix("web") {
        concat(
          pathSingleSlash {
            getFromFile(s"$webDir/index.html")
          },
          getFromDirectory(webDir)
        )
      },

      // --------------------------------------------------------
      // Root path -> index.html
      // --------------------------------------------------------
      pathSingleSlash {
        getFromFile(s"$webDir/index.html")
      },

      // --------------------------------------------------------
      // API routes
      // --------------------------------------------------------
      pathPrefix("api") {
        concat(
          path("rovers") {
            get {
              val rovers = roverService.listRovers()
              complete(jsonEntity(rovers.asJson))
            }
          },
          path("trajectory" / Segment) { roverId =>
            get {
              val traj = trajectoryService.getTrajectory(roverId)
              complete(jsonEntity(traj.asJson))
            }
          },
          path("replay" / Segment) { roverId =>
            get {
              val replay = replayService.getReplay(roverId)
              complete(jsonEntity(replay.asJson))
            }
          },
          path("graph") {
            get {
              val graph = graphService.getGraph()
              complete(jsonEntity(graph.asJson))
            }
          }
        )
      }
    )
}
