package phd.adaptivecontrol.interaction

import scala.collection.mutable.ArrayBuffer

import phd.adaptivecontrol.model.{GeoEvent, Interaction, InteractionType}
import phd.adaptivecontrol.spatial.{SpatialIndex, SpatialOperations}


/**
 * ProximityDetector
 *
 * Detects non-critical spatial proximity interactions
 * using spatial indexing + metric validation.
 */
class ProximityDetector {

  def detect(
    events: Seq[GeoEvent],
    thresholdMeters: Double
  ): Seq[Interaction] = {

    val start = System.nanoTime()
    val interactions = ArrayBuffer.empty[Interaction]

    if (events.isEmpty) {
      println(s"[PROXIMITY] action=emptyInput threshold=$thresholdMeters")
      return Seq.empty
    }

    println(
      s"[PROXIMITY] action=start events=${events.size} threshold=$thresholdMeters"
    )

    // ------------------------------------------------------------
    // 1. Build spatial index
    // ------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------
    // 2. Neighbor search
    // ------------------------------------------------------------
    var totalNeighbors = 0
    var distanceComputations = 0

    events.foreach { e1 =>

      val neighbors =
        spatialIndex
          .queryRadius(e1.lat, e1.lon, thresholdMeters)
          .filter(_.objectId != e1.objectId)

      totalNeighbors += neighbors.size

      neighbors.foreach { e2 =>

        val distance = SpatialOperations.distance(e1, e2)
        distanceComputations += 1

        if (distance <= thresholdMeters && distance > 1e-6) {

          println(
            s"[PROXIMITY] pair=(${e1.objectId},${e2.objectId}) distance=$distance"
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
              "distance" -> distance.toString
            )
          )
        }
      }
    }

    // ------------------------------------------------------------
    // 3. Deduplication (A-B == B-A)
    // ------------------------------------------------------------
    val deduped =
      interactions
        .groupBy(_.objectIds.toSet)
        .map(_._2.head)
        .toSeq

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[PROXIMITY] summary events=${events.size} " +
      s"neighbors=$totalNeighbors distanceComputations=$distanceComputations " +
      s"interactionsRaw=${interactions.size} interactionsFinal=${deduped.size} " +
      s"timeMs=$elapsedMs"
    )

    deduped
  }
}
