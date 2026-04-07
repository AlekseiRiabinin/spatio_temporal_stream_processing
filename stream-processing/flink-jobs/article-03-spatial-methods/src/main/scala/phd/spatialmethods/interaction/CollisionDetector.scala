package phd.spatialmethods.interaction

import java.time.{Duration, Instant}
import scala.collection.mutable

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType, Trajectory}
import phd.spatialmethods.spatial.{KNN, SpatialIndex, SpatialOperations}
import phd.spatialmethods.temporal.TrajectoryBuilder


/**
 * CollisionDetector
 *
 * Detects potential collisions using:
 *  - SpatialIndex (fast radius queries)
 *  - KNN (local neighbor refinement)
 *  - SpatialOperations (distance computation)
 *  - TrajectoryBuilder (velocity + prediction)
 *
 * Scientific model:
 *  - distance threshold
 *  - time-to-collision (TTC)
 */
class CollisionDetector(
  kNeighbors: Int = 5,
  maxGap: Duration = Duration.ofSeconds(5),
  ttcThresholdSec: Double = 5.0
) {

  private val trajectoryBuilder = new TrajectoryBuilder(maxGap)

  def detect(
    events: Seq[GeoEvent],
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val interactions = mutable.ArrayBuffer.empty[Interaction]

    if (events.isEmpty) return Seq.empty

    // ------------------------------------------------------------------
    // 1. Build spatial index
    // ------------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------------
    // 2. Build trajectories per object
    // ------------------------------------------------------------------
    val trajectories = mutable.Map[String, Trajectory]()

    events.foreach { e =>
      val current = trajectories.get(e.objectId)
      val updated = trajectoryBuilder.updateTrajectory(current, e)
      trajectories.update(e.objectId, updated)
    }

    // ------------------------------------------------------------------
    // 3. For each event → find candidate neighbors
    // ------------------------------------------------------------------
    events.foreach { e1 =>

      // Step 3.1: spatial pre-filter (radius query)
      val spatialCandidates =
        spatialIndex.queryRadius(e1.lat, e1.lon, thresholdMeters)

      // Step 3.2: refine with KNN (limit to k nearest)
      val neighbors =
        KNN.findKNN(e1.lat, e1.lon, spatialCandidates, kNeighbors)
          .map(_._1)
          .filter(_.id != e1.id)

      neighbors.foreach { e2 =>

        // ------------------------------------------------------------------
        // 4. Distance check (SpatialOperations)
        // ------------------------------------------------------------------
        val distance = SpatialOperations.distance(e1, e2)

        if (distance <= thresholdMeters) {

          // ------------------------------------------------------------------
          // 5. Compute velocities from trajectories
          // ------------------------------------------------------------------
          val v1 = computeVelocity(trajectories.get(e1.objectId))
          val v2 = computeVelocity(trajectories.get(e2.objectId))

          val relativeSpeed = math.sqrt(
            math.pow(v1._1 - v2._1, 2) +
            math.pow(v1._2 - v2._2, 2)
          )

          // ------------------------------------------------------------------
          // 6. Time-to-collision (TTC)
          // ------------------------------------------------------------------
          val ttc =
            if (relativeSpeed > 1e-6) distance / relativeSpeed
            else Double.PositiveInfinity

          if (ttc <= ttcThresholdSec) {

            val lat = (e1.lat + e2.lat) / 2.0
            val lon = (e1.lon + e2.lon) / 2.0

            val timestamp: Instant =
              if (e1.timestamp.isAfter(e2.timestamp)) e1.timestamp else e2.timestamp

            interactions += Interaction(
              id = s"${e1.id}-${e2.id}-${timestamp.toEpochMilli}",
              interactionType = InteractionType.Collision,
              objectIds = Seq(e1.objectId, e2.objectId),
              timestamp = timestamp,
              lat = lat,
              lon = lon,
              severity = Some(1.0 / (ttc + 1e-6)),
              attributes = Map(
                "distance" -> distance.toString,
                "ttc" -> ttc.toString,
                "relative_speed" -> relativeSpeed.toString
              )
            )
          }
        }
      }
    }

    // ------------------------------------------------------------------
    // 7. Deduplicate interactions (A-B == B-A)
    // ------------------------------------------------------------------
    interactions
      .groupBy(i => i.objectIds.toSet)
      .map(_._2.head)
      .toSeq
  }

  /**
   * Compute velocity vector (vx, vy) from trajectory
   */
  private def computeVelocity(trajOpt: Option[Trajectory]): (Double, Double) = {

    trajOpt match {
      case Some(traj) =>
        val events = traj.sortedEvents

        if (events.length < 2) return (0.0, 0.0)

        val e1 = events(events.length - 2)
        val e2 = events.last

        val dt =
          (e2.timestamp.toEpochMilli - e1.timestamp.toEpochMilli) / 1000.0

        if (dt <= 0) return (0.0, 0.0)

        val dx = e2.lon - e1.lon
        val dy = e2.lat - e1.lat

        (dx / dt, dy / dt)

      case None =>
        (0.0, 0.0)
    }
  }
}
