package phd.adaptivecontrol.interaction

import phd.adaptivecontrol.model.{GeoEvent, Interaction}


/**
 * InteractionEngine
 *
 * Orchestrates all interaction detectors:
 *  - Collision detection (physics-based TTC)
 *  - Proximity detection (distance-based)
 *  - Conflict detection (CPA-based prediction)
 *  - Swarm clustering (ST-DBSCAN)
 */
class InteractionEngine() {

  // ------------------------------------------------------------
  // Tunable parameters (system-wide defaults)
  // ------------------------------------------------------------
  private val collisionThresholdMeters = 5.0
  private val proximityThresholdMeters = 20.0
  private val conflictThresholdMeters = 10.0
  private val predictionHorizonSec = 5.0

  private val swarmEpsMeters = 15.0
  private val swarmMinPoints = 3

  // ------------------------------------------------------------
  // Stateless processing
  // ------------------------------------------------------------
  def process(events: Seq[GeoEvent]): Seq[Interaction] = {

    if (events == null || events.isEmpty)
      return Seq.empty

    val distinctObjects =
      events.map(_.objectId).distinct.size

    println(
      s"[INTERACTION ENGINE] events=${events.size} objects=$distinctObjects"
    )

    // ------------------------------------------------------------
    // Detectors (stateless per batch)
    // ------------------------------------------------------------
    val collisionDetector =
      new CollisionDetector(
        kNeighbors = 5,
        maxGapMs = 5000L,
        ttcThresholdSec = predictionHorizonSec
      )

    val proximityDetector =
      new ProximityDetector()

    val conflictDetector =
      new ConflictDetector(
        maxGapMs = 5000L,
        defaultHorizonSec = predictionHorizonSec,
        conflictThresholdMeters = conflictThresholdMeters
      )

    val swarmClustering =
      new SwarmClustering()

    // ------------------------------------------------------------
    // 1. Run detectors
    // ------------------------------------------------------------
    val collisions =
      collisionDetector.detect(
        events,
        collisionThresholdMeters
      )

    val proximity =
      proximityDetector.detect(
        events,
        proximityThresholdMeters
      )

    val conflicts =
      conflictDetector.detect(
        events,
        predictionHorizonSec,
        conflictThresholdMeters
      )

    val swarms =
      swarmClustering.detect(
        events,
        swarmEpsMeters,
        swarmMinPoints
      )

    // ------------------------------------------------------------
    // 2. Merge results
    // ------------------------------------------------------------
    collisions ++ proximity ++ conflicts ++ swarms
  }
}
