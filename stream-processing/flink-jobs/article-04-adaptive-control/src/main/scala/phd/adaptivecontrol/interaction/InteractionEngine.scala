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
class InteractionEngine() {

  // ------------------------------------------------------------
  // Default parameters
  // ------------------------------------------------------------
  private val collisionThreshold = 5.0
  private val proximityThreshold = 20.0
  private val conflictThreshold = 10.0
  private val predictionHorizon = 5.0

  private val swarmEps = 15.0
  private val swarmMinPoints = 3

  // ------------------------------------------------------------
  // Stateless processing
  // ------------------------------------------------------------
  def process(events: Seq[GeoEvent]): Seq[Interaction] = {

    if (events == null || events.isEmpty)
      return Seq.empty

    // ------------------------------------------------------------
    // Local instances per call (no shared state)
    // ------------------------------------------------------------
    val spatialIndex = SpatialIndex()

    val collisionDetector = new CollisionDetector()
    val proximityDetector = new ProximityDetector()
    val conflictDetector = new ConflictDetector()
    val swarmClustering = new SwarmClustering()

    // ------------------------------------------------------------
    // 1. Build spatial index
    // ------------------------------------------------------------
    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------
    // 2. Run detectors
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
