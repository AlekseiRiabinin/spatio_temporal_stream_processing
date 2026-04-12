package phd.spatialmethods.interaction

import scala.collection.mutable
import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType, Trajectory}
import phd.spatialmethods.spatial.SpatialOperations
import phd.spatialmethods.temporal.TrajectoryBuilder


/**
 * ConflictDetector
 *
 * Predicts future conflicts by projecting object motion forward
 * using linear velocity (meters/sec) and checking whether objects
 * will come within a threshold distance within a prediction horizon.
 *
 * Scientific model:
 *  - distance threshold (meters)
 *  - prediction horizon (seconds)
 *  - linear motion model using velocity from trajectory
 */
class ConflictDetector(
  maxGapMs: Long = 5000L
) {

  private val trajectoryBuilder = new TrajectoryBuilder(maxGapMs)

  /**
   * Predictive conflict detection
   *
   * @param events incoming events for the current window
   * @param horizonSec prediction horizon (seconds)
   * @param thresholdMeters conflict threshold (meters)
   */
  def detect(
    events: Seq[GeoEvent],
    horizonSec: Double,
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val start = System.nanoTime()
    val interactions = mutable.ArrayBuffer.empty[Interaction]

    if (events.isEmpty) {
      println(
        s"[CONFLICT] action=emptyInput threshold=$thresholdMeters horizon=$horizonSec"
      )
      return Seq.empty
    }

    println(
      s"[CONFLICT] action=start " +
      s"events=${events.size} " +
      s"threshold=$thresholdMeters " +
      s"horizon=$horizonSec"
    )

    // ------------------------------------------------------------
    // 1. Build trajectories
    // ------------------------------------------------------------
    val trajectories = mutable.Map[String, Trajectory]()

    events.foreach { e =>
      val updated = trajectoryBuilder.updateTrajectory(
        trajectories.get(e.objectId),
        e
      )
      trajectories.update(e.objectId, updated)
    }

    println(
      s"[CONFLICT] action=trajectoriesBuilt count=${trajectories.size}"
    )

    // ------------------------------------------------------------
    // 2. Pairwise predictive conflict detection
    // ------------------------------------------------------------
    val objects = trajectories.keys.toSeq

    for {
      i <- objects.indices
      j <- (i + 1) until objects.length
    } {
      val id1 = objects(i)
      val id2 = objects(j)

      val traj1 = trajectories(id1)
      val traj2 = trajectories(id2)

      val e1 = traj1.sortedEvents.last
      val e2 = traj2.sortedEvents.last

      // ----------------------------------------------------------
      // Compute velocities (m/s)
      // ----------------------------------------------------------
      val v1 = computeVelocity(traj1)
      val v2 = computeVelocity(traj2)

      val relativeSpeed =
        math.sqrt(math.pow(v1._1 - v2._1, 2) + math.pow(v1._2 - v2._2, 2))

      if (relativeSpeed <= 1e-6) {
        println(
          s"[CONFLICT] skipZeroSpeed pair=($id1,$id2)"
        )
      } else {

        // --------------------------------------------------------
        // Predict future positions using linear motion
        // --------------------------------------------------------
        val futurePositions: Seq[(Double, Double)] = (0 to 20).map { step =>
          val t = step * (horizonSec / 20.0)

          // Predict lat/lon using local tangent-plane approximation
          val lat1 = e1.lat + (v1._2 * t) / 6371000.0 * (180.0 / math.Pi)
          val lon1 = e1.lon + (v1._1 * t) /
            (6371000.0 * math.cos(math.toRadians(e1.lat))) * (180.0 / math.Pi)

          val lat2 = e2.lat + (v2._2 * t) / 6371000.0 * (180.0 / math.Pi)
          val lon2 = e2.lon + (v2._1 * t) /
            (6371000.0 * math.cos(math.toRadians(e2.lat))) * (180.0 / math.Pi)

          val d = SpatialOperations.distance(
            GeoEvent(
              id = s"pred-$id1",
              objectId = id1,
              timestamp = e1.timestamp,
              lon = lon1,
              lat = lat1,
              wkt = "",
              speed = None,
              heading = None
            ),
            GeoEvent(
              id = s"pred-$id2",
              objectId = id2,
              timestamp = e2.timestamp,
              lon = lon2,
              lat = lat2,
              wkt = "",
              speed = None,
              heading = None
            )
          )

          (t, d)
        }

        // --------------------------------------------------------
        // Check if any predicted distance is below threshold
        // --------------------------------------------------------
        val conflict = futurePositions.find { case (_, d) =>
          d <= thresholdMeters
        }

        conflict.foreach { case (t, d) =>
          val ts = math.max(e1.timestamp, e2.timestamp)

          interactions += Interaction(
            id = s"conf-$id1-$id2-$ts",
            interactionType = InteractionType.Conflict,
            objectIds = Seq(id1, id2),
            timestamp = ts,
            lat = (e1.lat + e2.lat) / 2.0,
            lon = (e1.lon + e2.lon) / 2.0,
            severity = Some(1.0 / (t + 1e-6)),
            attributes = Map(
              "predicted_distance" -> d.toString,
              "time_to_conflict" -> t.toString,
              "relative_speed" -> relativeSpeed.toString
            )
          )

          println(
            s"[CONFLICT] detected pair=($id1,$id2) t=$t distance=$d"
          )
        }
      }
    }

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[CONFLICT] summary " +
      s"events=${events.size} " +
      s"conflicts=${interactions.size} " +
      s"timeMs=$elapsedMs"
    )

    interactions.toSeq
  }

  /**
   * Compute velocity (meters/sec) from trajectory
   */
  private def computeVelocity(traj: Trajectory): (Double, Double) = {
    val events = traj.sortedEvents
    if (events.length < 2) return (0.0, 0.0)

    val e1 = events(events.length - 2)
    val e2 = events.last

    val dt = (e2.timestamp - e1.timestamp) / 1000.0
    if (dt <= 0) return (0.0, 0.0)

    val lat1 = math.toRadians(e1.lat)
    val lon1 = math.toRadians(e1.lon)
    val lat2 = math.toRadians(e2.lat)
    val lon2 = math.toRadians(e2.lon)

    val dLat = lat2 - lat1
    val dLon = lon2 - lon1

    val R = 6371000.0
    val meanLat = (lat1 + lat2) / 2.0

    val metersPerLat = R
    val metersPerLon = R * math.cos(meanLat)

    val dx = dLon * metersPerLon
    val dy = dLat * metersPerLat

    (dx / dt, dy / dt)
  }
}
