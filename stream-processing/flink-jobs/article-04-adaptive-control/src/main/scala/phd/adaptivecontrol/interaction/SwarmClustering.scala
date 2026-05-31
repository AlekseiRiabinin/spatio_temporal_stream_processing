package phd.adaptivecontrol.interaction

import scala.collection.mutable

import phd.adaptivecontrol.model.{GeoEvent, Interaction, InteractionType}
import phd.adaptivecontrol.spatial.{SpatialIndex, SpatialOperations}
import phd.adaptivecontrol.temporal.DensityEstimator


/**
 * SwarmClustering
 *
 * ST-DBSCAN-style clustering with:
 *  - deduplicated object IDs
 *  - stable visited tracking
 *  - DBSCAN-compatible core point logic
 *  - strict minPoints enforcement
 */
class SwarmClustering {

  def detect(
    events: Seq[GeoEvent],
    epsMeters: Double,
    minPoints: Int
  ): Seq[Interaction] = {

    val start = System.nanoTime()

    if (events.isEmpty) {
      println("[SWARM] action=emptyInput")
      return Seq.empty
    }

    println(
      s"[SWARM] action=start events=${events.size} eps=$epsMeters minPoints=$minPoints"
    )

    // ------------------------------------------------------------
    // Spatial index
    // ------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    println(
      s"[SWARM] action=spatialIndexBuilt size=${events.size}"
    )

    // ------------------------------------------------------------
    // Density filter
    // ------------------------------------------------------------
    val windowStart = events.map(_.timestamp).min
    val windowEnd   = events.map(_.timestamp).max

    val density =
      DensityEstimator.globalDensity(
        events,
        windowStart,
        windowEnd
      )

    println(
      s"[SWARM] action=globalDensity density=$density"
    )

    if (density <= 0) {
      println(
        "[SWARM] action=skip reason=lowDensity"
      )
      return Seq.empty
    }

    // ------------------------------------------------------------
    // Core DBSCAN state
    // ------------------------------------------------------------
    val visited =
      mutable.Set.empty[String]

    val clusters =
      mutable.ArrayBuffer.empty[Set[GeoEvent]]

    var neighborChecks = 0
    var clusterCount = 0

    // ------------------------------------------------------------
    // Main clustering loop
    // ------------------------------------------------------------
    events.foreach { seed =>

      if (!visited.contains(seed.objectId)) {

        visited += seed.objectId

        val neighbors =
          getNeighbors(
            seed,
            spatialIndex,
            epsMeters
          )

        neighborChecks += neighbors.size

        println(
          s"[SWARM] seed=${seed.objectId} neighbors=${neighbors.size}"
        )

        // --------------------------------------------------------
        // DBSCAN core-point condition
        // seed itself counts as one point
        // --------------------------------------------------------
        if (neighbors.size >= (minPoints - 1)) {

          val cluster =
            expandCluster(
              seed,
              neighbors,
              spatialIndex,
              visited,
              epsMeters,
              minPoints
            )

          val uniqueCluster =
            cluster
              .groupBy(_.objectId)
              .values
              .map(_.head)
              .toSet

          val uniqueObjectCount =
            uniqueCluster.size

          if (uniqueObjectCount >= minPoints) {

            clusters += uniqueCluster
            clusterCount += 1

            println(
              s"[SWARM] clusterFormed id=$clusterCount size=$uniqueObjectCount"
            )

          } else {

            println(
              s"[SWARM] clusterRejected size=$uniqueObjectCount minPoints=$minPoints"
            )
          }
        }
      }
    }

    // ------------------------------------------------------------
    // Final safety filter
    // ------------------------------------------------------------
    val finalClusters =
      clusters.filter(_.size >= minPoints)

    // ------------------------------------------------------------
    // Convert clusters -> interactions
    // ------------------------------------------------------------
    val interactions =
      finalClusters.map { cluster =>

        val objectIds =
          cluster
            .map(_.objectId)
            .toSeq
            .sorted

        val avgLat =
          cluster.map(_.lat).sum / cluster.size

        val avgLon =
          cluster.map(_.lon).sum / cluster.size

        val ts =
          cluster.map(_.timestamp).max

        Interaction(
          id = s"swarm-$ts-${objectIds.mkString("-").hashCode}",
          interactionType = InteractionType.Swarm,
          objectIds = objectIds,
          timestamp = ts,
          lat = avgLat,
          lon = avgLon,
          severity = Some(cluster.size.toDouble),
          attributes = Map(
            "clusterSize" -> cluster.size.toString,
            "density" -> density.toString
          )
        )
      }.toSeq

    val elapsedMs =
      (System.nanoTime() - start) / 1e6

    println(
      s"[SWARM] action=summary " +
      s"events=${events.size} " +
      s"clusters=${interactions.size} " +
      s"neighborChecks=$neighborChecks " +
      s"timeMs=$elapsedMs"
    )

    interactions
  }

  // ============================================================
  // Neighbor query helper
  // ============================================================
  private def getNeighbors(
    seed: GeoEvent,
    spatialIndex: SpatialIndex,
    epsMeters: Double
  ): Seq[GeoEvent] = {

    spatialIndex
      .queryRadius(
        seed.lat,
        seed.lon,
        epsMeters
      )
      .filter(_.objectId != seed.objectId)
      .filter(
        e =>
          SpatialOperations.distance(seed, e) <= epsMeters
      )
  }

  // ============================================================
  // Cluster expansion
  // ============================================================
  private def expandCluster(
    seed: GeoEvent,
    neighbors: Seq[GeoEvent],
    spatialIndex: SpatialIndex,
    visited: mutable.Set[String],
    epsMeters: Double,
    minPoints: Int
  ): Seq[GeoEvent] = {

    val cluster =
      mutable.ArrayBuffer.empty[GeoEvent]

    val queue =
      mutable.Queue(neighbors: _*)

    cluster += seed

    while (queue.nonEmpty) {

      val current =
        queue.dequeue()

      if (!visited.contains(current.objectId)) {

        visited += current.objectId

        val currentNeighbors =
          getNeighbors(
            current,
            spatialIndex,
            epsMeters
          )

        // DBSCAN core-point condition
        if (currentNeighbors.size >= (minPoints - 1)) {
          queue.enqueue(currentNeighbors: _*)
        }

        cluster += current
      }
    }

    cluster.toSeq
  }
}
