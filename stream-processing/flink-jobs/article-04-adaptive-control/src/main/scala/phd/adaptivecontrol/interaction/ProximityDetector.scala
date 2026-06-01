package phd.adaptivecontrol.interaction

import scala.collection.mutable

import phd.adaptivecontrol.model.{GeoEvent, Interaction, InteractionType}
import phd.adaptivecontrol.spatial.{SpatialIndex, SpatialOperations}


/**
 * ProximityDetector (UPDATED)
 *
 * Detects spatial proximity interactions with:
 *  - spatial indexing (radius search)
 *  - metric distance validation
 *  - basic temporal consistency filtering
 *
 * NOTE:
 * This is a "soft signal" detector (not risk-based like collision/conflict).
 */
class ProximityDetector {

  def detect(
    events: Seq[GeoEvent],
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val start = System.nanoTime()
    val interactions = mutable.ArrayBuffer.empty[Interaction]

    if (events.isEmpty) {
      println(s"[PROXIMITY] action=emptyInput threshold=$thresholdMeters")
      return Seq.empty
    }

    println(
      s"[PROXIMITY] action=start events=${events.size} threshold=$thresholdMeters"
    )

    // ------------------------------------------------------------
    // 1. Spatial index
    // ------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------
    // 2. Temporal window (basic consistency filter)
    // ------------------------------------------------------------
    val windowStart = events.map(_.timestamp).min
    val windowEnd   = events.map(_.timestamp).max

    val timeWindowMs = windowEnd - windowStart
    val maxAllowedTimeGap = math.max(5000L, timeWindowMs / 2)

    // ------------------------------------------------------------
    // 3. Detection
    // ------------------------------------------------------------
    val visitedPairs = mutable.Set[(String, String)]()

    var totalNeighbors = 0
    var distanceComputations = 0

    events.foreach { e1 =>

      val neighbors =
        spatialIndex
          .queryRadius(e1.lat, e1.lon, thresholdMeters)
          .filter(_.objectId != e1.objectId)

      totalNeighbors += neighbors.size

      neighbors.foreach { e2 =>

        // enforce pair uniqueness (A-B == B-A)
        val pair =
          if (e1.objectId < e2.objectId)
            (e1.objectId, e2.objectId)
          else
            (e2.objectId, e1.objectId)

        if (!visitedPairs.contains(pair)) {
          visitedPairs += pair

          // ------------------------------------------------------
          // temporal consistency check
          // ------------------------------------------------------
          val timeDiff = math.abs(e1.timestamp - e2.timestamp)

          if (timeDiff <= maxAllowedTimeGap) {

            val distance = SpatialOperations.distance(e1, e2)
            distanceComputations += 1

            if (distance > 1e-6 && distance <= thresholdMeters) {

              println(
                s"[PROXIMITY] pair=(${e1.objectId},${e2.objectId}) " +
                s"distance=$distance timeDiff=$timeDiff threshold=$thresholdMeters"
              )

              val ts = math.max(e1.timestamp, e2.timestamp)

              interactions += Interaction(
                id = s"prox-${e1.id}-${e2.id}-$ts",
                interactionType = InteractionType.Proximity,
                objectIds = Seq(e1.objectId, e2.objectId),
                timestamp = ts,
                lat = (e1.lat + e2.lat) / 2.0,
                lon = (e1.lon + e2.lon) / 2.0,
                severity = None,
                attributes = Map(
                  "distance" -> distance.toString,
                  "time_gap_ms" -> timeDiff.toString
                )
              )
            }
          }
        }
      }
    }

    // ------------------------------------------------------------
    // 4. Summary
    // ------------------------------------------------------------
    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[PROXIMITY] action=summary events=${events.size} " +
        s"neighbors=$totalNeighbors " +
        s"distanceComputations=$distanceComputations " +
        s"interactions=${interactions.size} " +
        s"timeMs=$elapsedMs"
    )

    interactions.toSeq
  }
}
