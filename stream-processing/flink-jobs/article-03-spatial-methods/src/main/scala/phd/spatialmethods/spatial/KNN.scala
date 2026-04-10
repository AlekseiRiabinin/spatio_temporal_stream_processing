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
    if (events.isEmpty) {
      println(
        s"[KNN] action=emptyInput k=$k queryLat=$queryLat queryLon=$queryLon"
      )
      return Seq.empty
    }

    val start = System.nanoTime()

    var distanceComputations = 0

    val heap = PriorityQueue.empty[(GeoEvent, Double)](
      Ordering.by[(GeoEvent, Double), Double](_._2).reverse
    )

    events.foreach { e =>
      val dist = GeometryUtils.haversineDistance(queryLat, queryLon, e.lat, e.lon)
      distanceComputations += 1

      if (heap.size < k) {
        heap.enqueue((e, dist))
      } else if (dist < heap.head._2) {
        heap.dequeue()
        heap.enqueue((e, dist))
      }
    }

    val result = heap.toSeq.sortBy(_._2)

    val end = System.nanoTime()
    val elapsedMs = (end - start) / 1e6

    println(
      s"[KNN] action=findKNN k=$k candidates=${events.size} " +
      s"distanceComputations=$distanceComputations " +
      s"returned=${result.size} timeMs=$elapsedMs " +
      s"queryLat=$queryLat queryLon=$queryLon"
    )

    result
  }
}
