package phd.adaptivecontrol.adaptive

import java.io.File

import ai.onnxruntime._

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{
  StreamFeatures,
  AdaptivePrediction,
  DecisionStrategy
}

object ONNXInference extends Serializable {

  // ============================================================
  // Bounds
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
  // Runtime
  // ============================================================

  @volatile
  private var initialized = false

  private var windowModelPath = "/opt/models/model_a_window.onnx"
  private var watermarkModelPath = "/opt/models/model_b_watermark.onnx"

  // ============================================================
  // ONNX Runtime
  // ============================================================

  @transient
  private var env: OrtEnvironment = _

  @transient
  private var windowSession: OrtSession = _

  @transient
  private var watermarkSession: OrtSession = _

  // ============================================================
  // Metadata
  // ============================================================

  private val FallbackModelVersion = "rule_based_v1"
  private val ONNXModelVersion = "onnx_v1"

  // ============================================================
  // Initialization
  // ============================================================

  def initialize(config: AdaptiveConfig): Unit = synchronized {

    if (initialized)
      return

    windowModelPath = config.windowModelPath
    watermarkModelPath = config.watermarkModelPath

    println(
      "[ONNX] action=initialize " +
      s"windowModel=$windowModelPath " +
      s"watermarkModel=$watermarkModelPath"
    )

    try {

      env = OrtEnvironment.getEnvironment()

      val options =
        new OrtSession.SessionOptions()

      if (new File(windowModelPath).exists()) {

        windowSession =
          env.createSession(windowModelPath, options)

        println(
          s"[ONNX] action=load type=window status=success path=$windowModelPath"
        )
      }

      if (new File(watermarkModelPath).exists()) {

        watermarkSession =
          env.createSession(watermarkModelPath, options)

        println(
          s"[ONNX] action=load type=watermark status=success path=$watermarkModelPath"
        )
      }

    } catch {

      case ex: Exception =>
        println(
          "[ONNX] action=load status=fallback " +
          s"reason=${ex.getMessage}"
        )
    }

    initialized = true

    println(
      s"[ONNX] action=status loaded=$isModelLoaded"
    )
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
    windowSession != null &&
    watermarkSession != null

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
