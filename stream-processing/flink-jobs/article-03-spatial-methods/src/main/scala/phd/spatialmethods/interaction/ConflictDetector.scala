package phd.spatialmethods.interaction

import java.time.{Duration, Instant}
import scala.collection.mutable

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType, Trajectory}
import phd.spatialmethods.temporal.{TrajectoryBuilder, TimeAggregation}
import phd.spatialmethods.spatial.SpatialOperations


/**
 * ConflictDetector
 *
 * Detects potential conflicts using:
 *  - TrajectoryBuilder (object trajectories)
 *  - TimeAggregation (temporal activity / rate)
 *  - SpatialOperations (distance checks)
 *
 * Scientific model:
 *  - future trajectory convergence (prediction horizon)
 *  - but NOT immediate collision (TTC > threshold implicitly)
 */
class ConflictDetector(
  maxGap: Duration = Duration.ofSeconds(5),
  minEventRate: Double = 0.1 // filter noise / inactive objects
) {

  private val trajectoryBuilder = new TrajectoryBuilder(maxGap)

  def detect(
    events: Seq[GeoEvent],
    predictionHorizonSec: Double,
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val start = System.nanoTime()

    val interactions = mutable.ArrayBuffer.empty[Interaction]

    if (events.isEmpty) {
      println(
        s"[CONFLICT] action=emptyInput " +
        s"horizon=$predictionHorizonSec " +
        s"threshold=$thresholdMeters"
      )
      return Seq.empty
    }

    println(
      s"[CONFLICT] action=start " +
      s"events=${events.size} " +
      s"horizon=$predictionHorizonSec " +
      s"threshold=$thresholdMeters"
    )

    // ------------------------------------------------------------------
    // 1. Build trajectories per object
    // ------------------------------------------------------------------
    val trajectories = mutable.Map[String, Trajectory]()

    events.foreach { e =>
      val current = trajectories.get(e.objectId)
      val updated = trajectoryBuilder.updateTrajectory(current, e)
      trajectories.update(e.objectId, updated)
    }

    println(
      s"[CONFLICT] action=trajectoriesBuilt count=${trajectories.size}"
    )

    val trajList = trajectories.values.toSeq

    // ------------------------------------------------------------------
    // 2. Temporal filtering (remove inactive / noisy objects)
    // ------------------------------------------------------------------
    val windowStart = events.map(_.timestamp).min
    val windowEnd   = events.map(_.timestamp).max

    val activeObjects = trajectories.filter { case (_, traj) =>
      val rate = TimeAggregation.eventRate(traj.events, windowStart, windowEnd)
      rate >= minEventRate
    }

    val activeTrajectories = activeObjects.values.toSeq

    println(
      s"[CONFLICT] action=temporalFilter " +
      s"active=${activeTrajectories.size} " +
      s"minRate=$minEventRate"
    )

    // ------------------------------------------------------------------
    // 3. Pairwise trajectory conflict detection
    // ------------------------------------------------------------------
    var distanceComputations = 0
    var conflictPairs = 0

    for {
      i <- activeTrajectories.indices
      j <- i + 1 until activeTrajectories.length
    } {

      val t1 = activeTrajectories(i)
      val t2 = activeTrajectories(j)

      val e1Opt = t1.sortedEvents.lastOption
      val e2Opt = t2.sortedEvents.lastOption

      if (e1Opt.isDefined && e2Opt.isDefined) {

        val e1 = e1Opt.get
        val e2 = e2Opt.get

        // ------------------------------------------------------------------
        // 4. Predict future positions using trajectory velocity
        // ------------------------------------------------------------------
        val (lat1Future, lon1Future) =
          predictFromTrajectory(t1, predictionHorizonSec)

        val (lat2Future, lon2Future) =
          predictFromTrajectory(t2, predictionHorizonSec)

        val futureE1 = e1.copy(lat = lat1Future, lon = lon1Future)
        val futureE2 = e2.copy(lat = lat2Future, lon = lon2Future)

        val futureDistance = SpatialOperations.distance(futureE1, futureE2)
        distanceComputations += 1

        println(
          s"[CONFLICT] pair=(${t1.objectId},${t2.objectId}) " +
          s"predictedDistance=$futureDistance " +
          s"horizon=$predictionHorizonSec"
        )

        // ------------------------------------------------------------------
        // 5. Conflict condition (future convergence)
        // ------------------------------------------------------------------
        if (futureDistance <= thresholdMeters) {

          conflictPairs += 1

          val lat = (lat1Future + lat2Future) / 2.0
          val lon = (lon1Future + lon2Future) / 2.0

          val timestamp: Instant =
            if (e1.timestamp.isAfter(e2.timestamp)) e1.timestamp else e2.timestamp

          interactions += Interaction(
            id = s"conf-${t1.objectId}-${t2.objectId}-${timestamp.toEpochMilli}",
            interactionType = InteractionType.Conflict,
            objectIds = Seq(t1.objectId, t2.objectId),
            timestamp = timestamp,
            lat = lat,
            lon = lon,
            severity = Some(1.0 / (futureDistance + 1e-6)),
            attributes = Map(
              "predictedDistance" -> futureDistance.toString,
              "horizonSec" -> predictionHorizonSec.toString
            )
          )

          println(
            s"[CONFLICT] detected " +
            s"pair=(${t1.objectId},${t2.objectId}) " +
            s"distance=$futureDistance"
          )
        }
      }
    }

    // ------------------------------------------------------------------
    // 6. Deduplicate interactions
    // ------------------------------------------------------------------
    val deduped =
      interactions
        .groupBy(i => i.objectIds.toSet)
        .map(_._2.head)
        .toSeq

    val end = System.nanoTime()
    val elapsedMs = (end - start) / 1e6

    println(
      s"[CONFLICT] action=summary trajectories=${trajectories.size} " +
      s"active=${activeTrajectories.size} distanceComputations=$distanceComputations " +
      s"conflictsRaw=${interactions.size} conflictsFinal=${deduped.size} " +
      s"timeMs=$elapsedMs"
    )

    deduped
  }

  /**
   * Predict future position using trajectory-derived velocity
   */
  private def predictFromTrajectory(
    traj: Trajectory,
    horizonSec: Double
  ): (Double, Double) = {

    val events = traj.sortedEvents

    if (events.length < 2) {
      val last = events.last
      return (last.lat, last.lon)
    }

    val e1 = events(events.length - 2)
    val e2 = events.last

    val dt =
      (e2.timestamp.toEpochMilli - e1.timestamp.toEpochMilli) / 1000.0

    if (dt <= 0) return (e2.lat, e2.lon)

    val dx = e2.lon - e1.lon
    val dy = e2.lat - e1.lat

    val vx = dx / dt
    val vy = dy / dt

    val futureLat = e2.lat + vy * horizonSec
    val futureLon = e2.lon + vx * horizonSec

    (futureLat, futureLon)
  }
}
