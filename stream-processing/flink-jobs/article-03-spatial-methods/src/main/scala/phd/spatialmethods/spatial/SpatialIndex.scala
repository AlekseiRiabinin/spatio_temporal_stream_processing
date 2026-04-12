package phd.spatialmethods.spatial

import scala.collection.mutable
import phd.spatialmethods.model.GeoEvent


/**
 * SpatialIndex
 *
 * Grid-based spatial index using local tangent-plane approximation.
 * All distances are in meters and consistent with SpatialOperations.
 */
class SpatialIndex(cellSizeMeters: Double = 100.0) {

  // Maps (gridX, gridY) -> events in that cell
  private val grid: mutable.Map[(Int, Int), mutable.ListBuffer[GeoEvent]] =
    mutable.Map.empty

  private val EarthRadius = 6371000.0

  /**
   * Convert lat/lon to local grid coordinates (meters)
   *
   * Uses local tangent-plane approximation:
   *   x = lon * cos(lat0) * R
   *   y = lat * R
   *
   * This avoids distortion and keeps grid uniform.
   */
  private def toGrid(lat: Double, lon: Double): (Int, Int) = {
    val latRad = math.toRadians(lat)
    val xMeters = lon * math.cos(latRad) * (math.Pi / 180.0) * EarthRadius
    val yMeters = lat * (math.Pi / 180.0) * EarthRadius
    ((xMeters / cellSizeMeters).toInt, (yMeters / cellSizeMeters).toInt)
  }

  /** Add a GeoEvent to the index */
  def insert(event: GeoEvent): Unit = {
    val key = toGrid(event.lat, event.lon)
    val cell = grid.getOrElseUpdate(key, mutable.ListBuffer.empty)
    cell += event

    println(
      s"[SPATIAL INDEX] action=insert cell=$key size=${cell.size} " +
      s"objectId=${event.objectId} lat=${event.lat} lon=${event.lon}"
    )
  }

  /** Remove a GeoEvent from the index */
  def remove(event: GeoEvent): Unit = {
    val key = toGrid(event.lat, event.lon)
    grid.get(key).foreach { buf =>
      buf -= event
      println(
        s"[SPATIAL INDEX] action=remove cell=$key size=${buf.size} " +
        s"objectId=${event.objectId}"
      )
    }
  }

  /**
   * Query events within radius (meters)
   */
  def queryRadius(lat: Double, lon: Double, radiusMeters: Double): Seq[GeoEvent] = {

    val start = System.nanoTime()

    val (gx, gy) = toGrid(lat, lon)

    // Check 3×3 neighborhood (safe for all radii <= cellSizeMeters)
    val cellsToCheck =
      for {
        dx <- -1 to 1
        dy <- -1 to 1
      } yield (gx + dx, gy + dy)

    val candidates = cellsToCheck.flatMap { key =>
      grid.getOrElse(key, Seq.empty)
    }

    // Filter by actual metric distance
    val filtered = candidates.filter { e =>
      SpatialOperations.distance(
        GeoEvent("query", "query", 0L, lon, lat, "", None, None),
        e
      ) <= radiusMeters
    }

    val elapsedMs = (System.nanoTime() - start) / 1e6

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
