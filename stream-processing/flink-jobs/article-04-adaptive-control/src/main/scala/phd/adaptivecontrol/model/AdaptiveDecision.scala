package phd.adaptivecontrol.model


/**
 * AdaptiveDecision
 *
 * Final executable runtime decision produced by
 * AdaptiveController after validation, safety checks,
 * and bounds enforcement.
 *
 * Supports:
 *   - rule-based control
 *   - ONNX inference
 *   - future ML models
 *   - future RL policies
 */
case class AdaptiveDecision(

  // ============================================================
  // Core adaptive control outputs
  // ============================================================

  watermarkDelayMs: Long,
  windowSizeMs: Long,
  allowedLatenessMs: Long,

  // ============================================================
  // Spatial interaction thresholds
  // ============================================================

  proximityThresholdMeters: Double,
  collisionThresholdMeters: Double,
  conflictThresholdMeters: Double,

  predictionHorizonSec: Double,

  // ============================================================
  // Decision metadata
  // ============================================================

  confidence: Double,
  strategy: DecisionStrategy,

  // ============================================================
  // ML traceability
  // ============================================================

  modelVersion: Option[String] = None,
  inferenceLatencyMs: Option[Double] = None,

  // ============================================================
  // Runtime metadata
  // ============================================================

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
  def isReliable: Boolean =
    confidence >= 0.8

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
      s"modelVersion=$modelVersion, " +
      s"inferenceLatencyMs=$inferenceLatencyMs, " +
      s"timestamp=$timestamp)"
  }
}


sealed trait DecisionStrategy

object DecisionStrategy {

  case object RuleBased extends DecisionStrategy
  case object ONNX extends DecisionStrategy
  case object ML extends DecisionStrategy
  case object RL extends DecisionStrategy
}
