package cityrover.visualizer.server

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}

import cityrover.visualizer.service._
import com.typesafe.config.Config

import io.circe.syntax._
import io.circe.generic.auto._


class Routes(config: Config) {

  private val roverService      = new RoverService(config)
  private val trajectoryService = new TrajectoryService(config)
  private val replayService     = new ReplayService(config)
  private val graphService      = new GraphService(config)

  private val webRoot: String =
    config.getString("visualizer.webRoot")

  // Helper: Circe JSON → Akka HTTP entity
  private def jsonEntity(json: io.circe.Json): HttpEntity.Strict =
    HttpEntity(ContentTypes.`application/json`, json.noSpaces)

  val routes: Route =
    concat(

      // --------------------------------------------------------
      // Static web assets
      // --------------------------------------------------------
      concat(
        pathSingleSlash {
          getFromResource(s"$webRoot/index.html")
        },
        getFromResourceDirectory(webRoot)
      ),

      // --------------------------------------------------------
      // API routes
      // --------------------------------------------------------
      pathPrefix("api") {
        concat(

          // /api/rovers
          path("rovers") {
            get {
              val rovers = roverService.listRovers()
              complete(jsonEntity(rovers.asJson))
            }
          },

          // /api/rovers/{id}/trajectory
          path("rovers" / Segment / "trajectory") { roverId =>
            get {
              val traj = trajectoryService.getTrajectory(roverId)
              complete(jsonEntity(traj.asJson))
            }
          },

          // /api/rovers/{id}/replay
          path("rovers" / Segment / "replay") { roverId =>
            get {
              val replay = replayService.getReplay(roverId)
              complete(jsonEntity(replay.asJson))
            }
          },

          // /api/graph
          pathPrefix("graph") {
            get {
              val graph = graphService.getGraph()
              complete(jsonEntity(graph.asJson))
            }
          }
        )
      }
    )
}
