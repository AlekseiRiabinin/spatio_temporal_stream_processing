package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model._


object AdaptiveController extends Serializable {

  // ============================================================
  // Configuration Bounds
  // ============================================================

  private val MinWindowMs = 1000L
  private val MaxWindowMs = 10000L

  private val MinWatermarkMs = 500L
  private val MaxWatermarkMs = 10000L

  // ============================================================
  // Default thresholds
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
    // If system is in FIXED mode -> no ML-driven behavior
    // ----------------------------------------------------------

    if (AdaptiveRuntimeState.isFixed) {

      return AdaptiveDecision(
        watermarkDelayMs = 3000L,
        windowSizeMs = 5000L,
        allowedLatenessMs = 3000L,
        proximityThresholdMeters = DefaultProximityThreshold,
        collisionThresholdMeters = DefaultCollisionThreshold,
        conflictThresholdMeters = DefaultConflictThreshold,
        predictionHorizonSec = DefaultPredictionHorizonSec,
        confidence = 1.0,
        strategy = DecisionStrategy.ONNX,
        modelVersion = None,
        inferenceLatencyMs = Some(0.0),
        timestamp = System.currentTimeMillis()
      )
    }

    // ----------------------------------------------------------
    // ADAPTIVE mode logic
    // ----------------------------------------------------------

    val safePrediction =
      if (prediction != null && prediction.isValid)
        prediction
      else
        AdaptivePrediction.empty()

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

    AdaptiveDecision(
      watermarkDelayMs = watermarkDelayMs,
      windowSizeMs = windowSizeMs,
      allowedLatenessMs = watermarkDelayMs,
      proximityThresholdMeters = DefaultProximityThreshold,
      collisionThresholdMeters = DefaultCollisionThreshold,
      conflictThresholdMeters = DefaultConflictThreshold,
      predictionHorizonSec = DefaultPredictionHorizonSec,
      confidence = math.max(0.0, math.min(1.0, safePrediction.confidence)),
      strategy = safePrediction.strategy,
      modelVersion = safePrediction.modelVersion,
      inferenceLatencyMs = safePrediction.inferenceLatencyMs,
      timestamp = System.currentTimeMillis()
    )
  }
}
