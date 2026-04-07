package phd.spatialmethods.interaction

import java.time.Instant
import scala.collection.mutable

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType}
import phd.spatialmethods.spatial.{SpatialIndex, SpatialOperations}
import phd.spatialmethods.temporal.DensityEstimator


/**
 * SwarmClustering
 *
 * Detects swarm behavior using ST-DBSCAN-like logic:
 *  - SpatialIndex (efficient neighbor search)
 *  - SpatialOperations (distance checks)
 *  - DensityEstimator (temporal density filtering)
 *
 * A swarm = dense cluster in space-time
 */
class SwarmClustering {

  def detect(
    events: Seq[GeoEvent],
    epsMeters: Double,
    minPoints: Int
  ): Seq[Interaction] = {

    val visited = mutable.Set.empty[String]
    val clusters = mutable.ArrayBuffer.empty[Seq[GeoEvent]]

    if (events.isEmpty) return Seq.empty

    // ------------------------------------------------------------------
    // 1. Build spatial index
    // ------------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    // ------------------------------------------------------------------
    // 2. Temporal density filtering (global)
    // ------------------------------------------------------------------
    val windowStart = events.map(_.timestamp).min
    val windowEnd   = events.map(_.timestamp).max

    val globalDensity =
      DensityEstimator.globalDensity(events, totalArea = 1.0, windowStart, windowEnd)

    // Optional: skip clustering if density too low
    if (globalDensity <= 0) return Seq.empty

    // ------------------------------------------------------------------
    // 3. DBSCAN-style clustering
    // ------------------------------------------------------------------
    events.foreach { event =>

      if (!visited.contains(event.id)) {
        visited += event.id

        val neighbors =
          spatialIndex
            .queryRadius(event.lat, event.lon, epsMeters)

        if (neighbors.size >= minPoints) {

          val cluster = expandCluster(
            event,
            neighbors,
            spatialIndex,
            visited,
            epsMeters,
            minPoints
          )

          clusters += cluster
        }
      }
    }

    // ------------------------------------------------------------------
    // 4. Convert clusters → Interaction
    // ------------------------------------------------------------------
    clusters.map { cluster =>

      val objectIds = cluster.map(_.objectId)

      val avgLat = cluster.map(_.lat).sum / cluster.size
      val avgLon = cluster.map(_.lon).sum / cluster.size

      val timestamp: Instant =
        cluster.map(_.timestamp).maxBy(_.toEpochMilli)

      Interaction(
        id = s"swarm-${timestamp.toEpochMilli}-${objectIds.hashCode()}",
        interactionType = InteractionType.Swarm,
        objectIds = objectIds,
        timestamp = timestamp,
        lat = avgLat,
        lon = avgLon,
        severity = Some(cluster.size.toDouble),
        attributes = Map(
          "clusterSize" -> cluster.size.toString,
          "density" -> globalDensity.toString
        )
      )
    }.toSeq
  }

  /**
   * Expand cluster (DBSCAN core logic)
   */
  private def expandCluster(
    seed: GeoEvent,
    neighbors: Seq[GeoEvent],
    spatialIndex: SpatialIndex,
    visited: mutable.Set[String],
    epsMeters: Double,
    minPoints: Int
  ): Seq[GeoEvent] = {

    val cluster = mutable.ArrayBuffer.empty[GeoEvent]
    val queue = mutable.Queue(neighbors: _*)

    cluster += seed

    while (queue.nonEmpty) {

      val current = queue.dequeue()

      if (!visited.contains(current.id)) {
        visited += current.id

        val currentNeighbors =
          spatialIndex.queryRadius(current.lat, current.lon, epsMeters)

        if (currentNeighbors.size >= minPoints) {
          queue.enqueue(currentNeighbors: _*)
        }
      }

      if (!cluster.exists(_.id == current.id)) {
        cluster += current
      }
    }

    cluster.toSeq
  }
}
