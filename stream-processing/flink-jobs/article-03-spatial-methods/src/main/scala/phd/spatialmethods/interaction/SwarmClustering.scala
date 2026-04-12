package phd.spatialmethods.interaction

import scala.collection.mutable

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType}
import phd.spatialmethods.spatial.{SpatialIndex, SpatialOperations}
import phd.spatialmethods.temporal.DensityEstimator


/**
 * SwarmClustering
 *
 * Detects swarm behavior using ST-DBSCAN-like logic:
 *  - SpatialIndex (efficient neighbor search)
 *  - SpatialOperations (distance checks in meters)
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

    val start = System.nanoTime()

    val visited = mutable.Set.empty[String]
    val clusters = mutable.ArrayBuffer.empty[Seq[GeoEvent]]

    if (events.isEmpty) {
      println(
        s"[SWARM] action=emptyInput eps=$epsMeters minPoints=$minPoints"
      )
      return Seq.empty
    }

    println(
      s"[SWARM] action=start events=${events.size} eps=$epsMeters minPoints=$minPoints"
    )

    // ------------------------------------------------------------
    // 1. Build spatial index
    // ------------------------------------------------------------
    val spatialIndex = SpatialIndex()
    events.foreach(spatialIndex.insert)

    println(
      s"[SWARM] action=spatialIndexBuilt size=${events.size}"
    )

    // ------------------------------------------------------------
    // 2. Temporal density filtering
    // ------------------------------------------------------------
    val windowStartMs = events.map(_.timestamp).min
    val windowEndMs   = events.map(_.timestamp).max

    val globalDensity =
      DensityEstimator.globalDensity(events, totalArea = 1.0, windowStartMs, windowEndMs)

    println(
      s"[SWARM] action=globalDensity density=$globalDensity"
    )

    if (globalDensity <= 0) {
      println("[SWARM] action=skip reason=lowDensity")
      return Seq.empty
    }

    // ------------------------------------------------------------
    // 3. DBSCAN-style clustering
    // ------------------------------------------------------------
    var clusterCount = 0
    var neighborChecks = 0

    events.foreach { seed =>

      if (!visited.contains(seed.id)) {
        visited += seed.id

        // Initial neighbors (filter same-object)
        val neighbors =
          spatialIndex
            .queryRadius(seed.lat, seed.lon, epsMeters)
            .filter(_.objectId != seed.objectId)
            .filter(e => SpatialOperations.distance(seed, e) <= epsMeters)

        neighborChecks += neighbors.size

        println(
          s"[SWARM] seed=${seed.objectId} neighbors=${neighbors.size}"
        )

        if (neighbors.size >= minPoints) {

          val cluster = expandCluster(
            seed,
            neighbors,
            spatialIndex,
            visited,
            epsMeters,
            minPoints
          )

          clusterCount += 1

          println(
            s"[SWARM] clusterFormed id=$clusterCount size=${cluster.size}"
          )

          clusters += cluster
        }
      }
    }

    // ------------------------------------------------------------
    // 4. Convert clusters → Interaction
    // ------------------------------------------------------------
    val interactions = clusters.map { cluster =>

      val objectIds = cluster.map(_.objectId)

      val avgLat = cluster.map(_.lat).sum / cluster.size
      val avgLon = cluster.map(_.lon).sum / cluster.size

      val ts = cluster.map(_.timestamp).max

      println(
        s"[SWARM] clusterSummary size=${cluster.size} avgLat=$avgLat avgLon=$avgLon"
      )

      Interaction(
        id = s"swarm-$ts-${objectIds.hashCode()}",
        interactionType = InteractionType.Swarm,
        objectIds = objectIds,
        timestamp = ts,
        lat = avgLat,
        lon = avgLon,
        severity = Some(cluster.size.toDouble),
        attributes = Map(
          "clusterSize" -> cluster.size.toString,
          "density" -> globalDensity.toString
        )
      )
    }.toSeq

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[SWARM] action=summary events=${events.size} clusters=${clusters.size} " +
      s"neighborChecks=$neighborChecks timeMs=$elapsedMs"
    )

    interactions
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

    println(
      s"[SWARM] expand start seed=${seed.objectId} initialNeighbors=${neighbors.size}"
    )

    while (queue.nonEmpty) {

      val current = queue.dequeue()

      if (!visited.contains(current.id)) {
        visited += current.id

        val currentNeighbors =
          spatialIndex
            .queryRadius(current.lat, current.lon, epsMeters)
            .filter(_.objectId != current.objectId)
            .filter(e => SpatialOperations.distance(current, e) <= epsMeters)

        println(
          s"[SWARM] expand current=${current.objectId} neighbors=${currentNeighbors.size}"
        )

        if (currentNeighbors.size >= minPoints) {
          queue.enqueue(currentNeighbors: _*)
        }
      }

      if (!cluster.exists(_.id == current.id)) {
        cluster += current
      }
    }

    println(
      s"[SWARM] expand complete seed=${seed.objectId} clusterSize=${cluster.size}"
    )

    cluster.toSeq
  }
}
