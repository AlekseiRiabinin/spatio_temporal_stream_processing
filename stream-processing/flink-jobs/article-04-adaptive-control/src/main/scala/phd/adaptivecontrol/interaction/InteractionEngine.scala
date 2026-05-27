package phd.adaptivecontrol.interaction

import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.spatial.SpatialIndex


/**
 * InteractionEngine
 *
 * Pure domain service for spatio-temporal interaction detection.
 *
 * Responsibilities:
 *   - spatial indexing (candidate organization)
 *   - delegating to interaction detectors
 *   - merging interaction results
 *
 * IMPORTANT:
 * This class is intentionally free of Flink dependencies
 * to support:
 *   - unit testing
 *   - ML evaluation
 *   - adaptive control integration (Article 04)
 */
class InteractionEngine(

  collisionDetector: CollisionDetector =
    new CollisionDetector(),

  proximityDetector: ProximityDetector =
    new ProximityDetector(),

  conflictDetector: ConflictDetector =
    new ConflictDetector(),

  swarmClustering: SwarmClustering =
    new SwarmClustering(),

  spatialIndex: SpatialIndex =
    SpatialIndex()

) {

  // ------------------------------------------------------------
  // Default parameters (can later become adaptive inputs)
  // ------------------------------------------------------------
  private val collisionThreshold = 5.0
  private val proximityThreshold = 20.0
  private val conflictThreshold = 10.0
  private val predictionHorizon = 5.0

  private val swarmEps = 15.0
  private val swarmMinPoints = 3

  /**
   * Process a batch of GeoEvents and detect interactions
   */
  def process(events: Seq[GeoEvent]): Seq[Interaction] = {

    if (events == null || events.isEmpty)
      return Seq.empty

    // ------------------------------------------------------------
    // 1. Update spatial index
    // ------------------------------------------------------------
    spatialIndex.clear()

    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------
    // 2. Run interaction detectors
    // ------------------------------------------------------------
    val collisions =
      collisionDetector.detect(events, collisionThreshold)

    val proximity =
      proximityDetector.detect(events, proximityThreshold)

    val conflicts =
      conflictDetector.detect(events, predictionHorizon, conflictThreshold)

    val swarms =
      swarmClustering.detect(events, swarmEps, swarmMinPoints)

    // ------------------------------------------------------------
    // 3. Merge results
    // ------------------------------------------------------------
    collisions ++ proximity ++ conflicts ++ swarms
  }
}
