package phd.spatialmethods.interaction

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType}
import phd.spatialmethods.util.GeometryUtils


/**
 * Detects proximity interactions between objects
 * based on a configurable distance threshold.
 *
 * Unlike collisions, proximity does not imply risk —
 * it represents spatial closeness (e.g., coordination, grouping).
 */
class ProximityDetector {

  def detect(
      events: Seq[GeoEvent],
      thresholdMeters: Double
  ): Seq[Interaction] = {

    val interactions = ArrayBuffer.empty[Interaction]

    for {
      i <- events.indices
      j <- i + 1 until events.length
    } {
      val e1 = events(i)
      val e2 = events(j)

      val distance =
        GeometryUtils.haversineDistance(e1.lat, e1.lon, e2.lat, e2.lon)

      if (distance <= thresholdMeters) {

        // midpoint of interaction
        val lat = (e1.lat + e2.lat) / 2.0
        val lon = (e1.lon + e2.lon) / 2.0

        val timestamp: Instant =
          if (e1.timestamp.isAfter(e2.timestamp)) e1.timestamp else e2.timestamp

        interactions += Interaction(
          id = s"prox-${e1.id}-${e2.id}-${timestamp.toEpochMilli}",
          interactionType = InteractionType.Proximity,
          objectIds = Seq(e1.id, e2.id),
          timestamp = timestamp,
          lat = lat,
          lon = lon,
          severity = None, // proximity is neutral
          attributes = Map(
            "distance" -> distance.toString
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
}
