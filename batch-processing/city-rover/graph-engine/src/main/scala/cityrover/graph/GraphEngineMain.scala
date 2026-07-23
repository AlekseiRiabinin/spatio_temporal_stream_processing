package cityrover.graph

import com.typesafe.config.{Config, ConfigFactory}

import cityrover.graph.loader.OSMLoader
import cityrover.graph.builder.RoadGraphBuilder
import cityrover.graph.spatial.SpatialIndexBuilder
import cityrover.graph.speed.SpeedLimitResolver
import cityrover.graph.serialize.GraphSerializer


/**
  * GraphEngineMain orchestrates the full OSM → rover graph pipeline.
  *
  * Steps:
  *   1. Load OSM data (Postgres primary, PBF fallback)
  *   2. Build directed rover graph
  *   3. Resolve rover speed limits
  *   4. Build spatial index (STRtree)
  *   5. Serialize nodes, edges, spatial index
  */
object GraphEngineMain extends App {

  // ---------------------------------------------------------------------------
  // Load config
  // ---------------------------------------------------------------------------

  private val config: Config =
    ConfigFactory.load().getConfig("cityrover.graph-engine")

  // Postgres config (injected via Docker ENV)
  private val pgHost     = config.getString("postgres.host")
  private val pgPort     = config.getInt("postgres.port")
  private val pgDatabase = config.getString("postgres.database")
  private val pgUser     = config.getString("postgres.user")
  private val pgPassword = config.getString("postgres.password")

  // PBF fallback
  private val inputPbf: String = config.getString("inputPbf")

  // Output directory (MUST match Docker volume mount)
  private val outputDir: String = config.getString("outputDir")

  println("[GraphEngine] Starting graph build pipeline")
  println(s"[GraphEngine] Output directory: $outputDir")

  // ---------------------------------------------------------------------------
  // 1. Load raw OSM data (Postgres primary, PBF fallback)
  // ---------------------------------------------------------------------------

  println("[GraphEngine] Loading OSM data (Postgres primary, PBF fallback)…")

  println(s"[GraphEngine] PostgreSQL host     : $pgHost")
  println(s"[GraphEngine] PostgreSQL port     : $pgPort")
  println(s"[GraphEngine] PostgreSQL database : $pgDatabase")
  println(s"[GraphEngine] PostgreSQL user     : $pgUser")
  println(s"[GraphEngine] PBF fallback        : $inputPbf")

  val osmData =
    try {

      println("[GraphEngine] Attempting PostgreSQL load...")

      val data = OSMLoader.loadFromPostgres(
        host = pgHost,
        port = pgPort,
        db   = pgDatabase,
        user = pgUser,
        pass = pgPassword
      )

      println("[GraphEngine] PostgreSQL load succeeded.")
      data

    } catch {

      case ex: Throwable =>

        println("[GraphEngine] PostgreSQL load failed!")
        println(s"[GraphEngine] Exception: ${ex.getClass.getName}")
        println(s"[GraphEngine] Message  : ${ex.getMessage}")
        ex.printStackTrace()

        println("[GraphEngine] Falling back to PBF parsing...")

        OSMLoader.loadFromPbf(inputPbf)
    }

  // ---------------------------------------------------------------------------
  // 2. Build directed rover graph
  // ---------------------------------------------------------------------------

  val roadGraph = RoadGraphBuilder.build(osmData)

  println(
    s"[GraphEngine] Road graph built: " +
    s"nodes=${roadGraph.nodes.size}, edges=${roadGraph.edges.size}"
  )

  // ---------------------------------------------------------------------------
  // 3. Resolve rover speed limits
  // ---------------------------------------------------------------------------

  val graphWithSpeed = SpeedLimitResolver.apply(roadGraph)

  println("[GraphEngine] Speed limits resolved")

  // ---------------------------------------------------------------------------
  // 4. Build spatial index
  // ---------------------------------------------------------------------------

  val spatialIndex = SpatialIndexBuilder.build(graphWithSpeed)

  println("[GraphEngine] Spatial index built")

  // ---------------------------------------------------------------------------
  // 5. Serialize outputs
  // ---------------------------------------------------------------------------

  GraphSerializer.write(
    outputDir = outputDir,
    nodes = graphWithSpeed.nodes,
    edges = graphWithSpeed.edges,
    spatialIndex = spatialIndex
  )

  println(s"[GraphEngine] Graph successfully written to: $outputDir")
  println("[GraphEngine] Completed.")
}
