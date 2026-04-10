package phd.spatialmethods.spatial

import scala.collection.mutable
import phd.spatialmethods.model.GeoEvent
import phd.spatialmethods.util.GeometryUtils


/**
 * Simple spatial index for GeoEvents.
 * Divides the space into grid cells for fast neighborhood queries.
 */
class SpatialIndex(cellSizeMeters: Double = 100.0) {

  // Maps (gridX, gridY) -> events in that cell
  private val grid: mutable.Map[(Int, Int), mutable.ListBuffer[GeoEvent]] =
    mutable.Map.empty

  /**
   * Converts lat/lon to approximate grid coordinates
   */
  private def toGrid(lat: Double, lon: Double): (Int, Int) = {
    val earthCirc = 40075000.0 // meters
    val x = (lon + 180.0) / 360.0 * earthCirc
    val y = (lat + 90.0) / 180.0 * earthCirc
    ((x / cellSizeMeters).toInt, (y / cellSizeMeters).toInt)
  }

  /** Add a GeoEvent to the index */
  def insert(event: GeoEvent): Unit = {
    val key = toGrid(event.lat, event.lon)
    val cell = grid.getOrElseUpdate(key, mutable.ListBuffer.empty)
    cell += event

    println(
      s"[SPATIAL INDEX] action=insert cell=($key) size=${cell.size} " +
      s"objectId=${event.objectId} lat=${event.lat} lon=${event.lon}"
    )
  }

  /** Remove a GeoEvent from the index */
  def remove(event: GeoEvent): Unit = {
    val key = toGrid(event.lat, event.lon)
    grid.get(key).foreach { buf =>
      buf --= Seq(event)

      println(
        s"[SPATIAL INDEX] action=remove cell=($key) size=${buf.size} " +
        s"objectId=${event.objectId}"
      )
    }
  }

  /** Query events within radius (meters) of a point */
  def queryRadius(lat: Double, lon: Double, radiusMeters: Double): Seq[GeoEvent] = {

    val start = System.nanoTime()

    val (gx, gy) = toGrid(lat, lon)
    val cellsToCheck = (-1 to 1).flatMap { dx =>
      (-1 to 1).map(dy => (gx + dx, gy + dy))
    }

    val candidates = cellsToCheck.flatMap { key =>
      grid.getOrElse(key, Seq.empty)
    }

    val filtered = candidates.filter { e =>
      val dist = GeometryUtils.haversineDistance(lat, lon, e.lat, e.lon)
      dist <= radiusMeters
    }

    val end = System.nanoTime()
    val elapsedMs = (end - start) / 1e6

    println(
      s"[SPATIAL INDEX] action=queryRadius center=($gx,$gy) " +
      s"radius=$radiusMeters candidates=${candidates.size} " +
      s"returned=${filtered.size} timeMs=$elapsedMs"
    )

    filtered
  }

  /** Clear the index */
  def clear(): Unit = {
    grid.clear()
    println("[SPATIAL INDEX] action=clear")
  }
}

object SpatialIndex {
  def apply(cellSizeMeters: Double = 100.0): SpatialIndex =
    new SpatialIndex(cellSizeMeters)
}
