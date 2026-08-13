package cityrover.visualizer.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}


class HttpServer(config: Config) {

  private val log = LoggerFactory.getLogger(getClass)

  // ------------------------------------------------------------
  // Akka system + execution context
  // ------------------------------------------------------------
  implicit val system: ActorSystem = ActorSystem("rover-map-visualizer")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = Materializer(system)

  // ------------------------------------------------------------
  // Load routes
  // ------------------------------------------------------------
  private val routes: Route = new Routes(config).routes

  // ------------------------------------------------------------
  // Start server
  // ------------------------------------------------------------
  def start(): Unit = {
    val host = config.getString("server.host")
    val port = config.getInt("server.port")

    log.info(s"[HttpServer] Binding to $host:$port")

    val binding: Future[Http.ServerBinding] =
      Http().newServerAt(host, port).bind(routes)

    binding.onComplete {
      case Success(b) =>
        log.info(s"[HttpServer] Successfully started at ${b.localAddress}")
      case Failure(ex) =>
        log.error("[HttpServer] Failed to start HTTP server", ex)
        system.terminate()
    }
  }
}
