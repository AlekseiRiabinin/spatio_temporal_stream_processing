package phd.spatialmethods.interaction

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
 *  - distance threshold (meters)
 *  - time-to-collision (TTC)
 */
class CollisionDetector(
  kNeighbors: Int = 5,
  maxGapMs: Long = 5000L,
  ttcThresholdSec: Double = 5.0
) {

  private val trajectoryBuilder = new TrajectoryBuilder(maxGapMs)

  def detect(
    events: Seq[GeoEvent],
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val start = System.nanoTime()
    val interactions = mutable.ArrayBuffer.empty[Interaction]

    if (events.isEmpty) {
      println(
        s"[COLLISION] action=emptyInput " +
        s"threshold=$thresholdMeters " +
        s"k=$kNeighbors " +
        s"ttcThreshold=$ttcThresholdSec"
      )
      return Seq.empty
    }

    println(
      s"[COLLISION] action=start " +
      s"events=${events.size} " +
      s"threshold=$thresholdMeters " +
      s"k=$kNeighbors " +
      s"ttcThreshold=$ttcThresholdSec"
    )

    // ------------------------------------------------------------
    // 1. Spatial index
    // ------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    println(s"[COLLISION] action=spatialIndexBuilt size=${events.size}")

    // ------------------------------------------------------------
    // 2. Trajectories
    // ------------------------------------------------------------
    val trajectories = mutable.Map[String, Trajectory]()

    events.foreach { e =>
      val updated = trajectoryBuilder.updateTrajectory(
        trajectories.get(e.objectId),
        e
      )
      trajectories.update(e.objectId, updated)
    }

    println(s"[COLLISION] action=trajectoriesBuilt count=${trajectories.size}")

    // ------------------------------------------------------------
    // 3. Pairwise detection
    // ------------------------------------------------------------
    var spatialCandidatesTotal = 0
    var knnCandidatesTotal = 0
    var distanceComputations = 0

    events.foreach { e1 =>

      // ------------------------------------------------------------
      // Spatial candidates (raw radius search)
      // ------------------------------------------------------------
      val spatialCandidates =
        spatialIndex
          .queryRadius(e1.lat, e1.lon, thresholdMeters)
          .filter(_.objectId != e1.objectId) // remove self-object events

      spatialCandidatesTotal += spatialCandidates.size

      if (spatialCandidates.nonEmpty) {

        // ----------------------------------------------------------
        // KNN refinement (only on non-self candidates)
        // ----------------------------------------------------------
        val neighbors =
          KNN.findKNN(e1.lat, e1.lon, spatialCandidates, kNeighbors)
            .map(_._1)

        knnCandidatesTotal += neighbors.size

        neighbors.foreach { e2 =>

          // --------------------------------------------------------
          // Distance (must be meters)
          // --------------------------------------------------------
          val distance = SpatialOperations.distance(e1, e2)
          distanceComputations += 1

          println(
            s"[COLLISION] pair=(${e1.objectId},${e2.objectId}) " +
            s"distance=$distance " +
            s"threshold=$thresholdMeters"
          )

          if (distance <= thresholdMeters) {

            // ------------------------------------------------------
            // Velocity computation (epoch millis)
            // ------------------------------------------------------
            val v1 = computeVelocity(trajectories.get(e1.objectId))
            val v2 = computeVelocity(trajectories.get(e2.objectId))

            val relativeSpeed =
              math.sqrt(
                math.pow(v1._1 - v2._1, 2) +
                math.pow(v1._2 - v2._2, 2)
              )

            // Skip zero-relative-speed pairs (TTC = ∞)
            if (relativeSpeed > 1e-6) {

              val ttc = distance / relativeSpeed

              println(
                s"[COLLISION] pair=(${e1.objectId},${e2.objectId}) " +
                s"ttc=$ttc " +
                s"relativeSpeed=$relativeSpeed"
              )

              if (ttc <= ttcThresholdSec) {

                val lat = (e1.lat + e2.lat) / 2.0
                val lon = (e1.lon + e2.lon) / 2.0
                val ts = math.max(e1.timestamp, e2.timestamp)

                interactions += Interaction(
                  id = s"col-${e1.id}-${e2.id}-$ts",
                  interactionType = InteractionType.Collision,
                  objectIds = Seq(e1.objectId, e2.objectId),
                  timestamp = ts,
                  lat = lat,
                  lon = lon,
                  severity = Some(1.0 / (ttc + 1e-6)),
                  attributes = Map(
                    "distance" -> distance.toString,
                    "ttc" -> ttc.toString,
                    "relative_speed" -> relativeSpeed.toString
                  )
                )

                println(
                  s"[COLLISION] detected " +
                  s"pair=(${e1.objectId},${e2.objectId}) " +
                  s"distance=$distance " +
                  s"ttc=$ttc"
                )
              }
            } else {
              println(
                s"[COLLISION] skipZeroSpeed pair=(${e1.objectId},${e2.objectId})"
              )
            }
          }
        }
      }
    }

    // ------------------------------------------------------------
    // 4. Deduplication
    // ------------------------------------------------------------
    val deduped =
      interactions
        .groupBy(i => i.objectIds.toSet)
        .map(_._2.head)
        .toSeq

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[COLLISION] summary events=${events.size} spatialCandidates=$spatialCandidatesTotal knnCandidates=$knnCandidatesTotal distanceComputations=$distanceComputations collisions=${deduped.size} timeMs=$elapsedMs"
    )

    deduped
  }

  /**
   * Velocity from trajectory (epoch millis)
   */
  private def computeVelocity(
    trajOpt: Option[Trajectory]
  ): (Double, Double) = {

    trajOpt match {
      case Some(traj) =>
        val events = traj.sortedEvents
        if (events.length < 2) return (0.0, 0.0)

        val e1 = events(events.length - 2)
        val e2 = events.last

        val dt = (e2.timestamp - e1.timestamp) / 1000.0
        if (dt <= 0) return (0.0, 0.0)

        val dx = e2.lon - e1.lon
        val dy = e2.lat - e1.lat

        (dx / dt, dy / dt)

      case None =>
        (0.0, 0.0)
    }
  }
}
