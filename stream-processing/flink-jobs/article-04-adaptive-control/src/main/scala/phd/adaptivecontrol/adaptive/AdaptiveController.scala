package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model._


/**
 * AdaptiveController
 *
 * Converts AdaptivePrediction into
 * executable AdaptiveDecision.
 *
 * Responsibilities:
 *   - safety validation
 *   - bounds enforcement
 *   - runtime constraints
 *   - decision construction
 *
 * Does NOT perform inference.
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
  // Runtime thresholds
  // ============================================================

  private val DefaultProximityThreshold = 50.0
  private val DefaultCollisionThreshold = 10.0
  private val DefaultConflictThreshold = 25.0

  private val DefaultPredictionHorizonSec = 30.0

  // ============================================================
  // Main decision entry point
  // ============================================================

  def decide(
    features: StreamFeatures,
    prediction: AdaptivePrediction
  ): AdaptiveDecision = {

    // ----------------------------------------------------------
    // Safety fallback
    // ----------------------------------------------------------

    val safePrediction =
      if (prediction.isValid) prediction
      else AdaptivePrediction.empty()

    // ----------------------------------------------------------
    // Clamp adaptive values
    // ----------------------------------------------------------

    val windowSizeMs =
      math.max(
        MinWindowMs,
        math.min(safePrediction.windowSizeMs, MaxWindowMs)
      )

    val watermarkDelayMs =
      math.max(
        MinWatermarkMs,
        math.min(safePrediction.watermarkDelayMs, MaxWatermarkMs)
      )

    // ----------------------------------------------------------
    // Build executable decision
    // ----------------------------------------------------------

    AdaptiveDecision(
      watermarkDelayMs = watermarkDelayMs,
      windowSizeMs = windowSizeMs,
      allowedLatenessMs = watermarkDelayMs,
      proximityThresholdMeters = DefaultProximityThreshold,
      collisionThresholdMeters = DefaultCollisionThreshold,
      conflictThresholdMeters = DefaultConflictThreshold,
      predictionHorizonSec = DefaultPredictionHorizonSec,
      confidence = safePrediction.confidence,
      strategy = safePrediction.strategy,
      modelVersion = safePrediction.modelVersion,
      inferenceLatencyMs = safePrediction.inferenceLatencyMs,
      timestamp = System.currentTimeMillis()
    )
  }
}
