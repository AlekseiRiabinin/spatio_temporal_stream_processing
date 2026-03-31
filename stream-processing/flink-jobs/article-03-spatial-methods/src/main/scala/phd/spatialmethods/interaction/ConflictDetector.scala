package phd.spatialmethods.interaction

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType}
import phd.spatialmethods.util.GeometryUtils


/**
 * Detects potential conflicts based on trajectory prediction.
 *
 * A conflict occurs when two objects are expected to be within
 * a dangerous distance in the near future.
 */
class ConflictDetector {

  def detect(
      events: Seq[GeoEvent],
      predictionHorizonSec: Double,
      thresholdMeters: Double
  ): Seq[Interaction] = {

    val interactions = ArrayBuffer.empty[Interaction]

    for {
      i <- events.indices
      j <- i + 1 until events.length
    } {
      val e1 = events(i)
      val e2 = events(j)

      // predict future positions
      val (lat1Future, lon1Future) = predictPosition(e1, predictionHorizonSec)
      val (lat2Future, lon2Future) = predictPosition(e2, predictionHorizonSec)

      val futureDistance = GeometryUtils.haversineDistance(
        lat1Future, lon1Future,
        lat2Future, lon2Future
      )

      if (futureDistance <= thresholdMeters) {

        val lat = (lat1Future + lat2Future) / 2.0
        val lon = (lon1Future + lon2Future) / 2.0

        val timestamp: Instant =
          if (e1.timestamp.isAfter(e2.timestamp)) e1.timestamp else e2.timestamp

        interactions += Interaction(
          id = s"conf-${e1.id}-${e2.id}-${timestamp.toEpochMilli}",
          interactionType = InteractionType.Conflict,
          objectIds = Seq(e1.id, e2.id),
          timestamp = timestamp,
          lat = lat,
          lon = lon,
          severity = Some(1.0 / (futureDistance + 1e-6)),
          attributes = Map(
            "predictedDistance" -> futureDistance.toString,
            "horizonSec" -> predictionHorizonSec.toString
          )
        )
      }
    }

    // remove duplicates (A-B == B-A)
    interactions
      .groupBy(i => i.objectIds.toSet)
      .map(_._2.head)
      .toSeq
  }

  /**
   * Predicts future position using simple linear motion model
   */
  private def predictPosition(
      e: GeoEvent,
      horizonSec: Double
  ): (Double, Double) = {

    val speed = e.speed.getOrElse(0.0)
    val headingRad = math.toRadians(e.heading.getOrElse(0.0))

    // velocity components (m/s)
    val vx = speed * math.cos(headingRad)
    val vy = speed * math.sin(headingRad)

    // conversion meters → degrees
    val metersPerDegLat = 111320.0
    val metersPerDegLon = 111320.0 * math.cos(math.toRadians(e.lat))

    val deltaLat = (vy * horizonSec) / metersPerDegLat
    val deltaLon = (vx * horizonSec) / metersPerDegLon

    val futureLat = e.lat + deltaLat
    val futureLon = e.lon + deltaLon

    (futureLat, futureLon)
  }
}
