package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model.{
  StreamFeatures,
  AdaptiveDecision
}


/**
 * AdaptiveController
 *
 * Converts runtime stream features into
 * adaptive watermark/window decisions.
 *
 * Phase 1:
 *   Rule-based adaptation
 *
 * Phase 2:
 *   ONNX-based adaptation
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
  // Default Parameters
  // ============================================================
  private val DefaultWindowMs = 5000L
  private val DefaultWatermarkMs = 3000L

  // ============================================================
  // Spatial Interaction Parameters
  // ============================================================
  private val DefaultProximityThreshold = 50.0
  private val DefaultCollisionThreshold = 10.0
  private val DefaultConflictThreshold = 25.0

  // ============================================================
  // Prediction Parameters
  // ============================================================
  private val DefaultPredictionHorizonSec = 30.0

  /**
   * Main decision entry point.
   */
  def decide(features: StreamFeatures): AdaptiveDecision = {

    var windowSizeMs = DefaultWindowMs
    var watermarkDelayMs = DefaultWatermarkMs

    // ----------------------------------------------------------
    // Disorder handling
    // ----------------------------------------------------------
    if (
      features.disorderRatio > 0.20 ||
      features.lateEventRatio > 0.20
    ) {

      watermarkDelayMs =
        math.min(DefaultWatermarkMs * 2, MaxWatermarkMs)
    }

    // ----------------------------------------------------------
    // High-load handling
    // ----------------------------------------------------------
    if (
      features.eventRate > 100.0 ||
      features.processingLatencyMs > 1000.0
    ) {

      windowSizeMs =
        math.min(DefaultWindowMs * 2, MaxWindowMs)
    }

    // ----------------------------------------------------------
    // Low-load optimization
    // ----------------------------------------------------------
    if (
      features.eventRate < 20.0 &&
      features.processingLatencyMs < 100.0
    ) {

      windowSizeMs =
        math.max(DefaultWindowMs / 2, MinWindowMs)

      watermarkDelayMs =
        math.max(DefaultWatermarkMs / 2, MinWatermarkMs)
    }

    // ----------------------------------------------------------
    // Interaction-aware adaptation
    // ----------------------------------------------------------
    if (
      features.collisionRate > 0.05 ||
      features.conflictRate > 0.05
    ) {

      watermarkDelayMs =
        math.min(watermarkDelayMs + 1000L, MaxWatermarkMs)
    }

    AdaptiveDecision(
      watermarkDelayMs = watermarkDelayMs,
      windowSizeMs = windowSizeMs,

      allowedLatenessMs = watermarkDelayMs,

      proximityThresholdMeters = DefaultProximityThreshold,
      collisionThresholdMeters = DefaultCollisionThreshold,
      conflictThresholdMeters = DefaultConflictThreshold,

      predictionHorizonSec = DefaultPredictionHorizonSec,

      confidence = calculateConfidence(features),

      strategy = "rule_based",

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
