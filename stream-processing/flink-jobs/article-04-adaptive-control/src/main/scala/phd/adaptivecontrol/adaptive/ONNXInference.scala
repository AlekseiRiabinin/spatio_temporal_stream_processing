package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{
  StreamFeatures,
  AdaptivePrediction,
  DecisionStrategy
}


/**
 * ONNXInference
 *
 * ML inference layer.
 *
 * Current state:
 *   Rule-based fallback.
 *
 * Future state:
 *   ONNX Runtime integration using:
 *     - model_a_window.onnx
 *     - model_b_watermark.onnx
 */
object ONNXInference extends Serializable {

  // ============================================================
  // Prediction bounds
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

  // ============================================================
  // Runtime configuration
  // ============================================================

  @volatile
  private var initialized = false

  private var windowModelPath = "/opt/models/model_a_window.onnx"
  private var watermarkModelPath = "/opt/models/model_b_watermark.onnx"

  // ============================================================
  // Metadata
  // ============================================================

  private val FallbackModelVersion = "rule_based_v1"

  // ============================================================
  // Initialization
  // ============================================================

  def initialize(config: AdaptiveConfig): Unit = synchronized {

    if (!initialized) {

      windowModelPath =
        config.windowModelPath

      watermarkModelPath =
        config.watermarkModelPath

      println(
        "[ONNX] action=initialize " +
        s"windowModel=$windowModelPath " +
        s"watermarkModel=$watermarkModelPath"
      )

      initialized = true
    }
  }

  // ============================================================
  // Prediction
  // ============================================================

  def predict(features: StreamFeatures): AdaptivePrediction = {

    val startNs = System.nanoTime()

    // ----------------------------------------------------------
    // Rule-based fallback
    // ----------------------------------------------------------

    var windowSizeMs =
      if (features.adaptiveWindowSizeMs > 0)
        features.adaptiveWindowSizeMs
      else
        DefaultWindowMs

    var watermarkDelayMs =
      if (features.adaptiveWatermarkDelayMs > 0)
        features.adaptiveWatermarkDelayMs
      else
        DefaultWatermarkMs

    if (
      features.disorderRatio > 0.20 ||
      features.lateEventRatio > 0.20
    ) {
      watermarkDelayMs =
        math.min(watermarkDelayMs * 2, MaxWatermarkMs)
    }

    if (
      features.eventRate > 100.0 ||
      features.processingLatencyMs > 1000.0
    ) {
      windowSizeMs =
        math.min(windowSizeMs * 2, MaxWindowMs)
    }

    if (
      features.eventRate < 20.0 &&
      features.processingLatencyMs < 100.0
    ) {

      windowSizeMs =
        math.max(windowSizeMs / 2, MinWindowMs)

      watermarkDelayMs =
        math.max(watermarkDelayMs / 2, MinWatermarkMs)
    }

    if (
      features.collisionRate > 0.05 ||
      features.conflictRate > 0.05
    ) {
      watermarkDelayMs =
        math.min(watermarkDelayMs + 1000L, MaxWatermarkMs)
    }

    // ----------------------------------------------------------
    // Confidence
    // ----------------------------------------------------------

    var confidence = 1.0

    confidence -=
      features.disorderRatio * 0.5

    confidence -=
      features.lateEventRatio * 0.3

    confidence =
      math.max(0.0, math.min(confidence, 1.0))

    // ----------------------------------------------------------
    // Latency
    // ----------------------------------------------------------

    val inferenceLatencyMs =
      (System.nanoTime() - startNs) / 1000000.0

    // ----------------------------------------------------------
    // Prediction
    // ----------------------------------------------------------

    AdaptivePrediction(
      windowSizeMs = windowSizeMs,
      watermarkDelayMs = watermarkDelayMs,
      confidence = confidence,
      strategy = DecisionStrategy.RuleBased,
      modelVersion = Some(FallbackModelVersion),
      inferenceLatencyMs = Some(inferenceLatencyMs)
    )
  }

  // ============================================================
  // Feature vector
  // ============================================================

  def createInputVector(features: StreamFeatures): Array[Float] =
    features.toVector

  // ============================================================
  // Runtime state
  // ============================================================

  def isModelLoaded: Boolean =
    false

  // ============================================================
  // Model paths
  // ============================================================

  def currentWindowModelPath: String =
    windowModelPath

  def currentWatermarkModelPath: String =
    watermarkModelPath

  // ============================================================
  // Status
  // ============================================================

  def status: String = {

    if (isModelLoaded) "onnx_loaded"
    else "rule_based_fallback"
  }
}
