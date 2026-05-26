package phd.adaptivecontrol.spatial

import scala.collection.mutable.PriorityQueue
import phd.adaptivecontrol.model.GeoEvent


object KNN {

  /**
    * Finds K nearest neighbors using max-heap pruning.
    *
    * @param queryLat latitude of query point
    * @param queryLon longitude of query point
    * @param events   candidate GeoEvents
    * @param k        number of neighbors
    */
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

    // ------------------------------------------------------------
    // Max-heap (worst candidate at head)
    // ------------------------------------------------------------
    val heap = PriorityQueue.empty[(GeoEvent, Double)](
      Ordering.by[(GeoEvent, Double), Double](_._2)
    )

    // ------------------------------------------------------------
    // Create reusable query point
    // ------------------------------------------------------------
    val queryEvent = GeoEvent(
      id = "query",
      objectId = "query",
      timestamp = 0L,
      lon = queryLon,
      lat = queryLat,
      wkt = "",
      speed = 0.0,
      heading = 0.0
    )

    // ------------------------------------------------------------
    // KNN scan
    // ------------------------------------------------------------
    events.foreach { e =>

      // skip identical object position (optional safety check)
      if (!(e.lat == queryLat && e.lon == queryLon)) {

        val dist = SpatialOperations.distance(queryEvent, e)
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
      s"[KNN] action=findKNN k=$k " +
      s"candidates=${events.size} " +
      s"distanceComputations=$distanceComputations " +
      s"returned=${result.size} " +
      s"timeMs=$elapsedMs " +
      s"queryLat=$queryLat queryLon=$queryLon"
    )

    result
  }
}
