package phd.spatialmethods.spatial

import scala.collection.mutable.PriorityQueue
import phd.spatialmethods.model.GeoEvent


object KNN {

  def findKNN(
    queryLat: Double,
    queryLon: Double,
    events: Seq[GeoEvent],
    k: Int
  ): Seq[(GeoEvent, Double)] = {

    require(k > 0, "k must be positive")

    if (events.isEmpty) {
      println(
        s"[KNN] action=emptyInput k=$k queryLat=$queryLat queryLon=$queryLon"
      )
      return Seq.empty
    }

    val start = System.nanoTime()
    var distanceComputations = 0

    // Max-heap: largest distance at head
    val heap = PriorityQueue.empty[(GeoEvent, Double)](
      Ordering.by[(GeoEvent, Double), Double](_._2)
    )

    events.foreach { e =>

      // Skip exact same coordinate (query point)
      if (!(e.lat == queryLat && e.lon == queryLon)) {

        val dist = SpatialOperations.distance(
          GeoEvent("query", "query", 0L, queryLon, queryLat, "", None, None),
          e
        )

        distanceComputations += 1

        if (heap.size < k) {
          heap.enqueue((e, dist))
        } else if (dist < heap.head._2) {
          heap.dequeue()
          heap.enqueue((e, dist))
        }
      }
    }

    val result = heap.toSeq.sortBy(_._2)

    val elapsedMs = (System.nanoTime() - start) / 1e6

    println(
      s"[KNN] action=findKNN k=$k candidates=${events.size} " +
      s"distanceComputations=$distanceComputations returned=${result.size} " +
      s"timeMs=$elapsedMs queryLat=$queryLat queryLon=$queryLon"
    )

    result
  }
}
