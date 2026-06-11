package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model._


/**
 * AdaptiveController
 *
 * Phase 1: Rule-based adaptation
 * Phase 2: ONNX/ML hybrid (ready)
 */
object AdaptiveController extends Serializable {

  // ============================================================
  // Configuration Bounds
  // ============================================================

  private val MinWindowMs = 1000L
  private val MaxWindowMs = 10000L

  private val MinWatermarkMs = 500L
  private val MaxWatermarkMs = 10000L

  // ============================================================
  // Defaults
  // ============================================================

  private val DefaultWindowMs = 5000L
  private val DefaultWatermarkMs = 3000L

  private val DefaultProximityThreshold = 50.0
  private val DefaultCollisionThreshold = 10.0
  private val DefaultConflictThreshold = 25.0

  private val DefaultPredictionHorizonSec = 30.0

  // ============================================================
  // MAIN DECISION FUNCTION
  // ============================================================

  def decide(features: StreamFeatures): AdaptiveDecision = {

    // ========================================================
    // BASE STATE
    // ========================================================

    var windowSizeMs =
      if (features.adaptiveWindowSizeMs > 0) features.adaptiveWindowSizeMs
      else DefaultWindowMs

    var watermarkDelayMs =
      if (features.adaptiveWatermarkDelayMs > 0) features.adaptiveWatermarkDelayMs
      else DefaultWatermarkMs

    // ========================================================
    // RULE-BASED ADAPTATION LOGIC
    // ========================================================

    if (features.disorderRatio > 0.20 || features.lateEventRatio > 0.20) {
      watermarkDelayMs =
        math.min(watermarkDelayMs * 2, MaxWatermarkMs)
    }

    if (features.eventRate > 100.0 || features.processingLatencyMs > 1000.0) {
      windowSizeMs =
        math.min(windowSizeMs * 2, MaxWindowMs)
    }

    if (features.eventRate < 20.0 && features.processingLatencyMs < 100.0) {
      windowSizeMs =
        math.max(windowSizeMs / 2, MinWindowMs)

      watermarkDelayMs =
        math.max(watermarkDelayMs / 2, MinWatermarkMs)
    }

    if (features.collisionRate > 0.05 || features.conflictRate > 0.05) {
      watermarkDelayMs =
        math.min(watermarkDelayMs + 1000L, MaxWatermarkMs)
    }

    // ========================================================
    // FINAL CLAMPING
    // ========================================================

    windowSizeMs =
      math.max(MinWindowMs, math.min(windowSizeMs, MaxWindowMs))

    watermarkDelayMs =
      math.max(MinWatermarkMs, math.min(watermarkDelayMs, MaxWatermarkMs))

    // ========================================================
    // CONFIDENCE
    // ========================================================

    val confidence = calculateConfidence(features)

    // ========================================================
    // BUILD DECISION
    // ========================================================

    AdaptiveDecision(
      watermarkDelayMs = watermarkDelayMs,
      windowSizeMs = windowSizeMs,
      allowedLatenessMs = watermarkDelayMs,

      proximityThresholdMeters = DefaultProximityThreshold,
      collisionThresholdMeters = DefaultCollisionThreshold,
      conflictThresholdMeters = DefaultConflictThreshold,

      predictionHorizonSec = DefaultPredictionHorizonSec,

      confidence = confidence,

      strategy = DecisionStrategy.RuleBased,

      // ======================================================
      // ML TRACEABILITY
      // ======================================================
      modelVersion = None,
      featureVectorHash = None,
      inferenceLatencyMs = None,

      timestamp = System.currentTimeMillis()
    )
  }

  // ============================================================
  // Confidence estimation
  // ============================================================

  private def calculateConfidence(features: StreamFeatures): Double = {

    var confidence = 1.0

    confidence -= features.disorderRatio * 0.5
    confidence -= features.lateEventRatio * 0.3

    math.max(0.0, math.min(1.0, confidence))
  }
}
