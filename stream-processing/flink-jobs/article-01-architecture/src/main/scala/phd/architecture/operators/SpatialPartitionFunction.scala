package phd.architecture.operators

import org.apache.flink.api.java.functions.KeySelector
import phd.architecture.model.{Event, SpatialPartition}
import phd.architecture.util.GeometryUtils
import phd.architecture.metrics.Metrics


object SpatialPartitionFunction {

  /**
   * π(e) → p
   * Spatial partitioning using geohash over event geometry.
   * Emits Prometheus metrics without modifying the job graph.
   */
  def byGeohash(precision: Int): KeySelector[Event, SpatialPartition] =
    new KeySelector[Event, SpatialPartition] {

      override def getKey(event: Event): SpatialPartition = {

        // compute geohash
        val geohash = GeometryUtils.toGeohash(event.geometry, precision)
        val partition = SpatialPartition(geohash, precision)

        partition
      }
    }
}
