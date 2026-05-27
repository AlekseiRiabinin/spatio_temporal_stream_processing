package phd.adaptivecontrol.model


/**
 * AdaptiveDecision
 *
 * Represents adaptive runtime-control decisions
 * produced by:
 *   - heuristic controllers
 *   - ML inference
 *   - ONNX models
 *   - reinforcement learning policies
 *
 * Used for:
 *   - watermark adaptation
 *   - adaptive window resizing
 *   - disorder tolerance control
 *   - runtime optimization
 */
case class AdaptiveDecision(
  watermarkDelayMs: Long,
  windowSizeMs: Long,
  allowedLatenessMs: Long,
  proximityThresholdMeters: Double,
  collisionThresholdMeters: Double,
  conflictThresholdMeters: Double,
  predictionHorizonSec: Double,
  confidence: Double,
  strategy: String,
  timestamp: Long
) {

  /**
   * Decision validity.
   */
  def isValid: Boolean = {
    watermarkDelayMs >= 0 &&
    windowSizeMs > 0 &&
    allowedLatenessMs >= 0 &&
    confidence >= 0.0 &&
    confidence <= 1.0
  }

  /**
   * High-confidence decision.
   */
  def isReliable: Boolean = {
    confidence >= 0.8
  }

  override def toString: String = {
    s"AdaptiveDecision(" +
      s"watermarkDelayMs=$watermarkDelayMs, " +
      s"windowSizeMs=$windowSizeMs, " +
      s"allowedLatenessMs=$allowedLatenessMs, " +
      s"proximityThresholdMeters=$proximityThresholdMeters, " +
      s"collisionThresholdMeters=$collisionThresholdMeters, " +
      s"conflictThresholdMeters=$conflictThresholdMeters, " +
      s"predictionHorizonSec=$predictionHorizonSec, " +
      s"confidence=$confidence, " +
      s"strategy=$strategy, " +
      s"timestamp=$timestamp)"
  }
}
