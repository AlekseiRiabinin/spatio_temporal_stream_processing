package phd.adaptivecontrol.adaptive

import scala.collection.JavaConverters._
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
          s"[ONNX] action=load status=fallback reason=${ex.getMessage}"
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
    var tensor: OnnxTensor = null

    try {

      println(
        "[ONNX] debug " +
        s"initialized=$initialized " +
        s"windowSession=${windowSession != null} " +
        s"watermarkSession=${watermarkSession != null}"
      )

      if (!isModelLoaded) {
        throw new IllegalStateException("ONNX models are not loaded")
      }

      // ========================================================
      // Feature vector
      // ========================================================

      val inputVector =
        createInputVector(features)

      println(
        s"[ONNX] action=inference inputSize=${inputVector.length}"
      )

      // ========================================================
      // Build ONNX tensor
      // ========================================================

      tensor =
        OnnxTensor.createTensor(env, Array(inputVector))

      // ======================================================
      // Execute window model
      // ======================================================

      val windowInputName =
        windowSession.getInputNames.iterator.next()

      val windowResult =
        windowSession.run(Map(windowInputName -> tensor).asJava)

      val predictedWindowMs =
        windowResult
          .get(0)
          .getValue
          .asInstanceOf[Array[Array[Float]]](0)(0)
          .toLong

      // ======================================================
      // Execute watermark model
      // ======================================================

      val watermarkInputName =
        watermarkSession.getInputNames.iterator.next()

      val watermarkResult =
        watermarkSession.run(Map(watermarkInputName -> tensor).asJava)

      val predictedWatermarkMs =
        watermarkResult
          .get(0)
          .getValue
          .asInstanceOf[Array[Array[Float]]](0)(0)
          .toLong

      val inferenceLatencyMs =
        (System.nanoTime() - startNs) / 1000000.0

      AdaptivePrediction(
        windowSizeMs = math.max(
          MinWindowMs,
          math.min(predictedWindowMs, MaxWindowMs)
        ),
        watermarkDelayMs = math.max(
          MinWatermarkMs,
          math.min(predictedWatermarkMs, MaxWatermarkMs)
        ),
        confidence = 1.0,
        strategy = DecisionStrategy.ONNX,
        modelVersion = Some(ONNXModelVersion),
        inferenceLatencyMs = Some(inferenceLatencyMs)
      )

    } catch {

      case ex: Throwable =>

        println(
          s"[ONNX] action=inference status=fallback reason=${ex.getMessage}"
        )

        predictRuleBased(features)

    } finally {
      if (tensor != null) tensor.close()
    }
  }

  // ============================================================
  // Rule-based fallback
  // ============================================================

  private def predictRuleBased(features: StreamFeatures): AdaptivePrediction = {

    val startNs = System.nanoTime()

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

    var confidence = 1.0

    confidence -= features.disorderRatio * 0.5
    confidence -= features.lateEventRatio * 0.3

    confidence =
      math.max(0.0, math.min(confidence, 1.0))

    val inferenceLatencyMs =
      (System.nanoTime() - startNs) / 1000000.0

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
