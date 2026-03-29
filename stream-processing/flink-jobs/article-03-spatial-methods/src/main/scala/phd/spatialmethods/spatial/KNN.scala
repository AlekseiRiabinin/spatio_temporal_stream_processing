package phd.spatialmethods.spatial

import phd.spatialmethods.model.GeoEvent
import scala.collection.mutable.PriorityQueue


/**
 * K-Nearest Neighbors for spatial events.
 * Finds k closest points to a given query point.
 */
object KNN {

  /**
   * Finds k nearest neighbors to a query point.
   *
   * @param queryLat Latitude of query point
   * @param queryLon Longitude of query point
   * @param events   Seq of GeoEvent to search
   * @param k        Number of neighbors to return
   * @return Seq of (GeoEvent, distanceMeters) sorted by distance ascending
   */
  def findKNN(
      queryLat: Double,
      queryLon: Double,
      events: Seq[GeoEvent],
      k: Int
  ): Seq[(GeoEvent, Double)] = {

    require(k > 0, "k must be positive")
    if (events.isEmpty) return Seq.empty

    // Max-heap to keep closest k neighbors
    val heap = PriorityQueue.empty[(GeoEvent, Double)](
      Ordering.by[(GeoEvent, Double), Double](_._2).reverse
    )

    events.foreach { e =>
      val dist = haversineDistance(queryLat, queryLon, e.lat, e.lon)
      if (heap.size < k) {
        heap.enqueue((e, dist))
      } else if (dist < heap.head._2) {
        heap.dequeue()
        heap.enqueue((e, dist))
      }
    }

    heap.toSeq.sortBy(_._2)
  }

  /**
   * Computes Haversine distance between two points in meters.
   */
  private def haversineDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R = 6371000.0 // Earth radius in meters
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
            math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) *
            math.sin(dLon / 2) * math.sin(dLon / 2)
    val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R * c
  }
}
