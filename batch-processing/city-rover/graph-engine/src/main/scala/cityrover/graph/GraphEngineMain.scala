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

  private val inputPbf: String = config.getString("inputPbf")
  private val outputDir: String = config.getString("outputDir")

  println("[GraphEngine] Starting graph build pipeline")
  println(s"[GraphEngine] Output directory: $outputDir")

  // ---------------------------------------------------------------------------
  // 1. Load raw OSM data (Postgres primary, PBF fallback)
  // ---------------------------------------------------------------------------

  println("[GraphEngine] Loading OSM data (Postgres primary, PBF fallback)…")

  val osmData = OSMLoader.load(inputPbf)

  println(
    s"[GraphEngine] Loaded OSM entities: " +
    s"nodes=${osmData.nodes.size}, " +
    s"ways=${osmData.ways.size}"
  )

  // ---------------------------------------------------------------------------
  // 2. Build directed rover graph
  // ---------------------------------------------------------------------------

  val roadGraph = RoadGraphBuilder.build(osmData)

  println(
    s"[GraphEngine] Road graph built: " +
    s"nodes=${roadGraph.nodes.size}, " +
    s"edges=${roadGraph.edges.size}"
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
