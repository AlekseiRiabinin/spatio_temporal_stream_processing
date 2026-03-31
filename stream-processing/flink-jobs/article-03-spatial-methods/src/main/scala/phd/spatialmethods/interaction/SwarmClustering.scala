package phd.spatialmethods.interaction

import java.time.Instant
import scala.collection.mutable

import phd.spatialmethods.model.{GeoEvent, Interaction, InteractionType}
import phd.spatialmethods.util.GeometryUtils


/**
 * Detects swarm behavior using density-based clustering (DBSCAN-like).
 *
 * A swarm is defined as a group of objects:
 *  - within epsMeters distance
 *  - having at least minPoints neighbors
 */
class SwarmClustering {

  def detect(
      events: Seq[GeoEvent],
      epsMeters: Double,
      minPoints: Int
  ): Seq[Interaction] = {

    val visited = mutable.Set.empty[String]
    val clusters = mutable.ArrayBuffer.empty[Seq[GeoEvent]]

    events.foreach { event =>
      if (!visited.contains(event.id)) {
        visited += event.id

        val neighbors = regionQuery(event, events, epsMeters)

        if (neighbors.size >= minPoints) {
          val cluster = expandCluster(event, neighbors, events, visited, epsMeters, minPoints)
          clusters += cluster
        }
      }
    }

    // Convert clusters → Interaction objects
    clusters.map { cluster =>
      val objectIds = cluster.map(_.id)

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
        severity = Some(cluster.size.toDouble), // size = importance
        attributes = Map(
          "clusterSize" -> cluster.size.toString
        )
      )
    }.toSeq
  }

  /**
   * Expands a cluster starting from a core point
   */
  private def expandCluster(
      seed: GeoEvent,
      neighbors: Seq[GeoEvent],
      events: Seq[GeoEvent],
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

        val currentNeighbors = regionQuery(current, events, epsMeters)

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

  /**
   * Finds neighbors within eps radius
   */
  private def regionQuery(
      center: GeoEvent,
      events: Seq[GeoEvent],
      epsMeters: Double
  ): Seq[GeoEvent] = {

    events.filter { e =>
      val dist = GeometryUtils.haversineDistance(
        center.lat,
        center.lon,
        e.lat,
        e.lon
      )
      dist <= epsMeters
    }
  }
}
