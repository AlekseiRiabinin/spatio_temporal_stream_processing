package phd.spatialmethods.interaction

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType}
import phd.spatialmethods.spatial.{SpatialIndex, SpatialOperations}


/**
 * ProximityDetector
 *
 * Detects proximity interactions using:
 *  - SpatialIndex (fast neighborhood queries)
 *  - SpatialOperations (distance computation)
 *
 * Scientific model:
 *  - ε-neighborhood (DBSCAN-style)
 *
 * Proximity represents spatial closeness without risk.
 */
class ProximityDetector {

  def detect(
    events: Seq[GeoEvent],
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val interactions = ArrayBuffer.empty[Interaction]

    if (events.isEmpty) return Seq.empty

    // ------------------------------------------------------------------
    // 1. Build spatial index
    // ------------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------------
    // 2. For each event → query neighbors within threshold
    // ------------------------------------------------------------------
    events.foreach { e1 =>

      val neighbors =
        spatialIndex
          .queryRadius(e1.lat, e1.lon, thresholdMeters)
          .filter(_.id != e1.id)

      neighbors.foreach { e2 =>

        // ------------------------------------------------------------------
        // 3. Distance check (SpatialOperations)
        // ------------------------------------------------------------------
        val distance = SpatialOperations.distance(e1, e2)

        if (distance <= thresholdMeters) {

          val lat = (e1.lat + e2.lat) / 2.0
          val lon = (e1.lon + e2.lon) / 2.0

          val timestamp: Instant =
            if (e1.timestamp.isAfter(e2.timestamp)) e1.timestamp else e2.timestamp

          interactions += Interaction(
            id = s"prox-${e1.id}-${e2.id}-${timestamp.toEpochMilli}",
            interactionType = InteractionType.Proximity,
            objectIds = Seq(e1.objectId, e2.objectId),
            timestamp = timestamp,
            lat = lat,
            lon = lon,
            severity = None,
            attributes = Map(
              "distance" -> distance.toString
            )
          )
        }
      }
    }

    // ------------------------------------------------------------------
    // 4. Deduplicate interactions (A-B == B-A)
    // ------------------------------------------------------------------
    interactions
      .groupBy(i => i.objectIds.toSet)
      .map(_._2.head)
      .toSeq
  }
}
