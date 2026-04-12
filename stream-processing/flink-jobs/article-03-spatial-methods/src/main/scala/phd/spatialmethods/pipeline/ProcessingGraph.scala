package phd.spatialmethods.pipeline

import phd.spatialmethods.model.{GeoEvent, Interaction}
import phd.spatialmethods.spatial.SpatialIndex
import phd.spatialmethods.interaction._


/**
 * ProcessingGraph defines a logical pipeline of spatial-temporal processing steps.
 */
class ProcessingGraph {

  private val spatialIndex = SpatialIndex()

  // Instantiate detectors
  private val collisionDetector = new CollisionDetector
  private val proximityDetector = new ProximityDetector
  private val conflictDetector = new ConflictDetector
  private val swarmClustering = new SwarmClustering

  /**
   * Configurable parameters
   */
  private val collisionThreshold = 5.0     // meters
  private val proximityThreshold = 20.0    // meters
  private val conflictThreshold = 10.0     // meters
  private val predictionHorizon = 5.0      // seconds

  private val swarmEps = 15.0              // meters
  private val swarmMinPoints = 3

  def process(events: Seq[GeoEvent]): Seq[Interaction] = {

    if (events.isEmpty) return Seq.empty

    // 1. Update spatial index
    spatialIndex.clear()
    events.foreach(spatialIndex.insert)

    // 2. Spatial filtering (candidate reduction)
    val filteredEvents = events

    // 3. Run detectors with FULL parameters
    val collisions = collisionDetector.detect(
      filteredEvents,
      collisionThreshold
    )

    val proximity = proximityDetector.detect(
      filteredEvents,
      proximityThreshold
    )

    val conflicts = conflictDetector.detect(
      filteredEvents,
      predictionHorizon,
      conflictThreshold
    )

    val swarms = swarmClustering.detect(
      filteredEvents,
      swarmEps,
      swarmMinPoints
    )

    // 4. Merge results
    collisions ++ proximity ++ conflicts ++ swarms
  }
}
