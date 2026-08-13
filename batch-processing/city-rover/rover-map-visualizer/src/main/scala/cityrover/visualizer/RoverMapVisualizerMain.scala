package cityrover.visualizer

import com.typesafe.config.{Config, ConfigFactory}
import cityrover.visualizer.server.HttpServer
import org.slf4j.LoggerFactory


object RoverMapVisualizerMain extends App {

  private val log = LoggerFactory.getLogger("RoverMapVisualizerMain")

  // ------------------------------------------------------------
  // Load configuration
  // ------------------------------------------------------------
  val config: Config = ConfigFactory.load()

  val host = config.getString("server.host")
  val port = config.getInt("server.port")

  log.info(s"[Visualizer] Starting Rover Map Visualizer on $host:$port")

  // ------------------------------------------------------------
  // Start HTTP server
  // ------------------------------------------------------------
  val server = new HttpServer(config)
  server.start()

  log.info("[Visualizer] Rover Map Visualizer is now running.")
}
