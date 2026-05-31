package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model.StreamFeatures


/**
 * FeatureExtractor
 *
 * Converts StreamProfiler state into ML-ready feature vector.
 * Fully decoupled from internal counters and timing logic.
 */
object FeatureExtractor extends Serializable {

  /**
   * Extract full ML feature vector from runtime profiler state.
   *
   * NOTE:
   * Processing latency is computed internally by StreamProfiler.
   */
  def extract(): StreamFeatures = {
    StreamProfiler.snapshot()
  }

  /**
   * Extract float vector for ONNX inference.
   *
   * Direct conversion from StreamFeatures → Array[Float].
   */
  def extractVector(): Array[Float] = {
    StreamProfiler.snapshot().toVector
  }
}
