package phd.adaptivecontrol.model


/**
 * AdaptiveDecision
 *
 * Reproducible adaptive control output with ML traceability.
 */
case class AdaptiveDecision(

  // ============================================================
  // Core adaptive control outputs
  // ============================================================
  watermarkDelayMs: Long,
  windowSizeMs: Long,
  allowedLatenessMs: Long,

  // ============================================================
  // Interaction thresholds (spatial control layer)
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
  featureVectorHash: Option[String] = None,
  inferenceLatencyMs: Option[Double] = None,

  timestamp: Long
) {

  /**
   * Decision validity check
   */
  def isValid: Boolean = {
    watermarkDelayMs >= 0 &&
    windowSizeMs > 0 &&
    allowedLatenessMs >= 0 &&
    confidence >= 0.0 &&
    confidence <= 1.0
  }

  /**
   * High-confidence decision
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
      s"featureVectorHash=$featureVectorHash, " +
      s"inferenceLatencyMs=$inferenceLatencyMs, " +
      s"timestamp=$timestamp)"
  }
}


sealed trait DecisionStrategy

object DecisionStrategy {

  case object RuleBased extends DecisionStrategy
  case object Heuristic extends DecisionStrategy
  case object ML extends DecisionStrategy
  case object ONNX extends DecisionStrategy
  case object RL extends DecisionStrategy
}
