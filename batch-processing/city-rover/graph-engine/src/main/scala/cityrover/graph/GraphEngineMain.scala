package cityrover.graph

import com.typesafe.config.{Config, ConfigFactory}
import cityrover.graph.loader.OSMLoader
import cityrover.graph.builder.RoadGraphBuilder
import cityrover.graph.spatial.SpatialIndexBuilder
import cityrover.graph.speed.SpeedLimitResolver
import cityrover.graph.serialize.GraphSerializer


/**
  * GraphEngineMain orchestrates the full OSM → road graph pipeline.
  *
  * Steps:
  *   1. Load OSM PBF
  *   2. Extract drivable ways + nodes
  *   3. Build directed road graph
  *   4. Resolve speed limits
  *   5. Build spatial index (R-tree / KD-tree)
  *   6. Serialize nodes, edges, metadata
  */
object GraphEngineMain extends App {

  // Load config
  private val config: Config =
    ConfigFactory.load().getConfig("cityrover.graph-engine")

  private val inputPbf: String = config.getString("inputPbf")
  private val outputDir: String = config.getString("outputDir")

  println(s"[GraphEngine] Loading OSM PBF: $inputPbf")

  // 1. Load raw OSM data
  val osmData = OSMLoader.load(inputPbf)
  println(s"[GraphEngine] Loaded OSM entities: nodes=${osmData.nodes.size}, ways=${osmData.ways.size}")

  // 2. Build road graph (directed edges)
  val roadGraph = RoadGraphBuilder.build(osmData)
  println(s"[GraphEngine] Road graph built: nodes=${roadGraph.nodes.size}, edges=${roadGraph.edges.size}")

  // 3. Resolve speed limits
  val graphWithSpeed = SpeedLimitResolver.apply(roadGraph)
  println(s"[GraphEngine] Speed limits resolved")

  // 4. Build spatial index
  val spatialIndex = SpatialIndexBuilder.build(graphWithSpeed)
  println(s"[GraphEngine] Spatial index built")

  // 5. Serialize outputs
  GraphSerializer.write(
    outputDir = outputDir,
    nodes = graphWithSpeed.nodes,
    edges = graphWithSpeed.edges,
    spatialIndex = spatialIndex
  )

  println(s"[GraphEngine] Graph successfully written to: $outputDir")
  println("[GraphEngine] Completed.")
}
