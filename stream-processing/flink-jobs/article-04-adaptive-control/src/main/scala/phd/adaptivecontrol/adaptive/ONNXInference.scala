package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model.{
  StreamFeatures,
  AdaptiveDecision
}


/**
 * ONNXInference
 *
 * Phase 1:
 *   Stub implementation
 *
 * Phase 2:
 *   ONNX Runtime integration
 *
 * Converts StreamFeatures into AdaptiveDecision
 * using a trained ML model.
 */
object ONNXInference extends Serializable {

  /**
   * Predict adaptive decision.
   *
   * Current implementation delegates
   * to the rule-based controller until
   * a trained ONNX model becomes available.
   */
  def predict(features: StreamFeatures): AdaptiveDecision = {
    AdaptiveController.decide(features)
  }

  /**
   * Convert features into ONNX input vector.
   *
   * Useful for future runtime integration.
   */
  def createInputVector(features: StreamFeatures): Array[Float] = {

    features.toVector
  }

  /**
   * Indicates whether an ONNX model
   * is currently loaded.
   */
  def isModelLoaded: Boolean = false

  /**
   * Model metadata.
   */
  def modelName: String =
    "adaptive-window-watermark.onnx"

  /**
   * Runtime status.
   */
  def status: String = {

    if (isModelLoaded)
      "loaded"
    else
      "rule_based_fallback"
  }
}
