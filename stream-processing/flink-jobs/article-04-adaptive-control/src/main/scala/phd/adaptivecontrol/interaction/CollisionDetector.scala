package phd.adaptivecontrol.interaction

import scala.collection.mutable

import phd.adaptivecontrol.model.{GeoEvent, Interaction, InteractionType}
import phd.adaptivecontrol.spatial.{KNN, SpatialIndex, SpatialOperations}
import phd.adaptivecontrol.temporal.TrajectoryBuilder
import phd.adaptivecontrol.model.Trajectory


/**
 * CollisionDetector
 *
 * Detects potential collisions using:
 *  - SpatialIndex (fast radius queries)
 *  - KNN (local neighbor refinement)
 *  - SpatialOperations (distance computation)
 *  - TrajectoryBuilder (velocity estimation)
 *
 * Article 04 role:
 *  → baseline interaction detector (pre-adaptation layer)
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
      println(s"[COLLISION] emptyInput threshold=$thresholdMeters")
      return Seq.empty
    }

    // ------------------------------------------------------------
    // 1. Spatial index
    // ------------------------------------------------------------
    val spatialIndex = new SpatialIndex()
    events.foreach(spatialIndex.insert)

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

    // ------------------------------------------------------------
    // 3. Pairwise detection
    // ------------------------------------------------------------
    var spatialCandidatesTotal = 0
    var knnCandidatesTotal = 0
    var distanceComputations = 0

    events.foreach { e1 =>

      val spatialCandidates =
        spatialIndex
          .queryRadius(e1.lat, e1.lon, thresholdMeters)
          .filter(_.objectId != e1.objectId)

      spatialCandidatesTotal += spatialCandidates.size

      if (spatialCandidates.nonEmpty) {

        val neighbors =
          KNN.findKNN(e1.lat, e1.lon, spatialCandidates, kNeighbors)
            .map(_._1)

        knnCandidatesTotal += neighbors.size

        neighbors.foreach { e2 =>

          val distance = SpatialOperations.distance(e1, e2)
          distanceComputations += 1

          if (distance <= thresholdMeters) {

            val v1 = computeVelocity(trajectories.get(e1.objectId))
            val v2 = computeVelocity(trajectories.get(e2.objectId))

            val relativeSpeed =
              math.sqrt(
                math.pow(v1._1 - v2._1, 2) +
                math.pow(v1._2 - v2._2, 2)
              )

            if (relativeSpeed > 1e-6) {

              val ttc = distance / relativeSpeed

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
              }
            }
          }
        }
      }
    }

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[COLLISION] summary events=${events.size} " +
      s"spatialCandidates=$spatialCandidatesTotal " +
      s"knnCandidates=$knnCandidatesTotal " +
      s"distanceComputations=$distanceComputations " +
      s"collisions=${interactions.size} timeMs=$elapsedMs"
    )

    interactions.toSeq
  }

  /**
   * Velocity estimation from trajectory
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

        val dxMeters = SpatialOperations.distanceLon(e1.lat, e1.lon, e2.lon)
        val dyMeters = SpatialOperations.distanceLat(e1.lat, e2.lat)

        val dxSigned = if (e2.lon > e1.lon) dxMeters else -dxMeters
        val dySigned = if (e2.lat > e1.lat) dyMeters else -dyMeters

        (dxSigned / dt, dySigned / dt)

      case None =>
        (0.0, 0.0)
    }
  }
}
