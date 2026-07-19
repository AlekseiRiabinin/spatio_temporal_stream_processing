package cityrover.sim.runtime

import com.typesafe.config.{Config, ConfigFactory}
import cityrover.sim.graph.GraphService
import cityrover.sim.rover.RoverController
import cityrover.sim.telemetry.{TelemetryGenerator, KafkaTelemetryProducer}
import cityrover.sim.model.{RoverState, TelemetryEvent}

import scala.concurrent.duration._
import scala.collection.mutable.ArrayBuffer


object RoverSimulatorMain extends App {

  // ---------------------------------------------------------------------------
  // Load config
  // ---------------------------------------------------------------------------

  private val config: Config =
    ConfigFactory.load().getConfig("cityrover.sim")

  private val numRovers: Int =
    config.getInt("numRovers")

  private val tickMillis: Long =
    config.getLong("tickMillis")

  private val durationMinutes: Int =
    config.getInt("simulationDurationMinutes")

  // ---------------------------------------------------------------------------
  // Graph service (loads nodes.parquet, edges.parquet, spatial-index.bin)
  // ---------------------------------------------------------------------------

  private val graphCfg = config.getConfig("graph")
  private val graphService = new GraphService(graphCfg)

  // ---------------------------------------------------------------------------
  // Kafka producer
  // ---------------------------------------------------------------------------

  private val kafkaCfg = config.getConfig("kafka")
  private val kafkaProducer = new KafkaTelemetryProducer(
    kafkaCfg.getString("bootstrapServers"),
    kafkaCfg.getString("topic")
  )

  // ---------------------------------------------------------------------------
  // Telemetry generator (no args now)
  // ---------------------------------------------------------------------------

  private val telemetryGen = new TelemetryGenerator

  // ---------------------------------------------------------------------------
  // Initialize rovers
  // ---------------------------------------------------------------------------

  private val rovers = ArrayBuffer[RoverController]()

  println(s"[Simulator] Initializing $numRovers rovers...")

  for (i <- 1 to numRovers) {
    val startNode = graphService.getRandomStartNode()
    val route = graphService.getRandomRoute(startNode, length = 20)

    val rover = new RoverController(
      roverId = s"rover-$i",
      route = route,
      graphService = graphService
    )

    rovers += rover
  }

  println(s"[Simulator] Starting simulation for $durationMinutes minutes...")

  val totalTicks =
    (durationMinutes.minutes.toMillis / tickMillis).toInt

  // ---------------------------------------------------------------------------
  // Main simulation loop
  // ---------------------------------------------------------------------------

  for (_ <- 1 to totalTicks) {

    val now = System.currentTimeMillis()

    rovers.foreach { rover =>
      val state: RoverState = rover.step(tickMillis)
      val event: TelemetryEvent = telemetryGen.toTelemetry(state, now)

      kafkaProducer.send(event)
    }

    Thread.sleep(tickMillis)
  }

  // ---------------------------------------------------------------------------
  // Shutdown
  // ---------------------------------------------------------------------------

  println("[Simulator] Simulation completed. Closing Kafka producer...")
  kafkaProducer.close()

  println("[Simulator] Rover simulator finished.")
}
