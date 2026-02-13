package phd.architecture.operators

import org.apache.flink.api.java.functions.KeySelector
import phd.architecture.model.{Event, SpatialPartition}
import phd.architecture.util.GeometryUtils


object SpatialPartitionFunction {

  /**
   * π(e) → p
   * Spatial partitioning using geohash over event geometry.
   */
  def byGeohash(precision: Int): KeySelector[Event, SpatialPartition] =
    new KeySelector[Event, SpatialPartition] {
      override def getKey(event: Event): SpatialPartition = {
        val geohash = GeometryUtils.toGeohash(event.geometry, precision)

        // --- Partition correctness log ---
        println(
          s"[partition] id=${event.id} geohash=$geohash precision=$precision"
        )

        SpatialPartition(geohash, precision)
      }
    }
}
