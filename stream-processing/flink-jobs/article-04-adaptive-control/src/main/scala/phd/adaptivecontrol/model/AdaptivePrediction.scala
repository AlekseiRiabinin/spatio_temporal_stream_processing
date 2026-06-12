package phd.adaptivecontrol.model


/**
 * AdaptivePrediction
 *
 * Raw prediction produced by:
 *   - rule-based predictors
 *   - ONNX models
 *   - ML models
 *   - RL policies
 *
 * Unlike AdaptiveDecision, this object contains
 * only predicted control values and inference metadata.
 *
 * Runtime validation, clamping, safety constraints,
 * and threshold management are handled by
 * AdaptiveController.
 */
case class AdaptivePrediction(

  // ============================================================
  // Predicted adaptive values
  // ============================================================

  windowSizeMs: Long,
  watermarkDelayMs: Long,

  // ============================================================
  // Prediction metadata
  // ============================================================

  confidence: Double,
  strategy: DecisionStrategy,
  modelVersion: Option[String] = None,
  inferenceLatencyMs: Option[Double] = None,
  timestamp: Long = System.currentTimeMillis()
) {

  /**
   * Basic prediction validity.
   */
  def isValid: Boolean =
    windowSizeMs > 0 &&
    watermarkDelayMs >= 0 &&
    confidence >= 0.0 &&
    confidence <= 1.0

  /**
   * High-confidence prediction.
   */
  def isReliable: Boolean =
    confidence >= 0.8

  override def toString: String =
    s"AdaptivePrediction(" +
      s"windowSizeMs=$windowSizeMs, " +
      s"watermarkDelayMs=$watermarkDelayMs, " +
      s"confidence=$confidence, " +
      s"strategy=$strategy, " +
      s"modelVersion=$modelVersion, " +
      s"inferenceLatencyMs=$inferenceLatencyMs, " +
      s"timestamp=$timestamp)"
}


object AdaptivePrediction {

  def empty(
    timestamp: Long = System.currentTimeMillis()
  ): AdaptivePrediction =
    AdaptivePrediction(
      windowSizeMs = 5000L,
      watermarkDelayMs = 3000L,
      confidence = 0.0,
      strategy = DecisionStrategy.RuleBased,
      timestamp = timestamp
    )
}
