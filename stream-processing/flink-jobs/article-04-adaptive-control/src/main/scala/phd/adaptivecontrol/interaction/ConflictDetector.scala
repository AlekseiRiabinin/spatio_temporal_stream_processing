package phd.adaptivecontrol.interaction

import scala.collection.mutable

import phd.adaptivecontrol.model.{GeoEvent, Interaction, InteractionType, Trajectory}
import phd.adaptivecontrol.spatial.SpatialOperations
import phd.adaptivecontrol.temporal.TrajectoryBuilder


/**
 * ConflictDetector (UPDATED)
 *
 * Predicts conflicts using:
 *  - trajectory-based velocity estimation
 *  - TTC (time-to-collision) model
 *  - deterministic pairwise evaluation
 *
 * FIXES:
 *  - removed simulation grid
 *  - unified with CollisionDetector physics model
 *  - stabilized severity scaling
 */
class ConflictDetector(
  maxGapMs: Long = 5000L,
  defaultHorizonSec: Double = 5.0,
  conflictThresholdMeters: Double = 10.0
) {

  private val trajectoryBuilder = new TrajectoryBuilder(maxGapMs)

  def detect(
    events: Seq[GeoEvent],
    horizonSec: Double,
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val start = System.nanoTime()
    val interactions = mutable.ArrayBuffer.empty[Interaction]

    if (events.isEmpty) {
      println("[CONFLICT] action=emptyInput")
      return Seq.empty
    }

    println(
      s"[CONFLICT] action=start events=${events.size} threshold=$thresholdMeters horizon=$horizonSec"
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
    // 2. Pairwise TTC-based detection (UNIFIED MODEL)
    // ------------------------------------------------------------
    val objects = trajectories.keys.toSeq

    var pairChecks = 0

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

      val v1 = computeVelocity(traj1)
      val v2 = computeVelocity(traj2)

      val relSpeed =
        math.sqrt(
          math.pow(v1._1 - v2._1, 2) +
          math.pow(v1._2 - v2._2, 2)
        )

      pairChecks += 1

      if (relSpeed > 1e-6) {

        val distance =
          SpatialOperations.distance(e1, e2)

        val ttc = distance / relSpeed

        if (ttc > 0 && ttc <= horizonSec && distance <= thresholdMeters) {

          val ts = math.max(e1.timestamp, e2.timestamp)

          // normalized severity (stable range 0..1)
          val severityScore =
            math.max(0.0, math.min(1.0, 1.0 - (ttc / horizonSec)))

          interactions += Interaction(
            id = s"conf-${e1.id}-${e2.id}-$ts",
            interactionType = InteractionType.Conflict,
            objectIds = Seq(id1, id2),
            timestamp = ts,
            lat = (e1.lat + e2.lat) / 2.0,
            lon = (e1.lon + e2.lon) / 2.0,
            severity = Some(severityScore),
            attributes = Map(
              "distance" -> distance.toString,
              "ttc" -> ttc.toString,
              "relative_speed" -> relSpeed.toString
            )
          )

          println(
            s"[CONFLICT] detected pair=($id1,$id2) ttc=$ttc distance=$distance"
          )
        }

      } else {
        println(
          s"[CONFLICT] skipZeroSpeed pair=($id1,$id2)"
        )
      }
    }

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[CONFLICT] summary events=${events.size} " +
      s"pairsChecked=$pairChecks conflicts=${interactions.size} " +
      s"timeMs=$elapsedMs"
    )

    interactions.toSeq
  }

  /**
   * Velocity estimation from trajectory (meters/sec)
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
