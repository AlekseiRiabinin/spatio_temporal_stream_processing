package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model.StreamFeatures


/**
 * FeatureExtractor
 *
 * Converts StreamProfiler state into ML-ready feature vector.
 * NO direct counter access allowed.
 */
object FeatureExtractor extends Serializable {

  /**
   * Extract full ML feature vector from runtime profiler state.
   */
  def extract(processingLatencyMs: Double = 0.0): StreamFeatures = {
    StreamProfiler.snapshot(processingLatencyMs)
  }

  /**
   * Extract float vector for ONNX inference
   */
  def extractVector(processingLatencyMs: Double = 0.0): Array[Float] = {
    StreamProfiler.snapshot(processingLatencyMs).toVector
  }
}
