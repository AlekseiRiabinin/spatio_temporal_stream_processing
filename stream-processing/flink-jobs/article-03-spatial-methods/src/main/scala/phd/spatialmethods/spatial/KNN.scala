package phd.spatialmethods.spatial

import scala.collection.mutable.PriorityQueue
import phd.spatialmethods.model.GeoEvent
import phd.spatialmethods.util.GeometryUtils


object KNN {

  def findKNN(
    queryLat: Double,
    queryLon: Double,
    events: Seq[GeoEvent],
    k: Int
  ): Seq[(GeoEvent, Double)] = {

    require(k > 0, "k must be positive")
    if (events.isEmpty) return Seq.empty

    val heap = PriorityQueue.empty[(GeoEvent, Double)](
      Ordering.by[(GeoEvent, Double), Double](_._2).reverse
    )

    events.foreach { e =>
      val dist = GeometryUtils.haversineDistance(queryLat, queryLon, e.lat, e.lon)

      if (heap.size < k) {
        heap.enqueue((e, dist))
      } else if (dist < heap.head._2) {
        heap.dequeue()
        heap.enqueue((e, dist))
      }
    }

    heap.toSeq.sortBy(_._2)
  }
}
