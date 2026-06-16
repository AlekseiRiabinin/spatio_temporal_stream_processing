package phd.adaptivecontrol.model


/**
 * AdaptivePrediction
 *
 * Raw prediction produced by:
 *   - rule-based predictors
 *   - ONNX models
 *   - ML models
 *   - RL policies
 *
 * This object represents ONLY predicted values + metadata.
 * It does NOT perform validation or clamping.
 *
 * IMPORTANT:
 * - Always preserve the origin of the prediction
 * - Never erase model provenance (ONNX vs RuleBased)
 */
case class AdaptivePrediction(

  // ============================================================
  // Predicted adaptive values
  // ============================================================

  windowSizeMs: Long,
  watermarkDelayMs: Long,

  // ============================================================
  // Prediction metadata
  // ============================================================

  confidence: Double,
  strategy: DecisionStrategy,

  /**
   * Explicit origin marker:
   *   - ONNX
   *   - RuleBased
   *   - Hybrid
   */
  source: PredictionSource = PredictionSource.RuleBased,

  modelVersion: Option[String] = None,
  inferenceLatencyMs: Option[Double] = None,
  timestamp: Long = System.currentTimeMillis()
) {

  /**
   * Basic prediction validity.
   */
  def isValid: Boolean =
    windowSizeMs > 0 &&
    watermarkDelayMs >= 0 &&
    confidence >= 0.0 &&
    confidence <= 1.0

  /**
   * High-confidence prediction.
   */
  def isReliable: Boolean =
    confidence >= 0.8

  /**
   * Strong signal: produced by ML model and valid.
   */
  def isMLBased: Boolean =
    source == PredictionSource.ONNX || source == PredictionSource.Hybrid

  override def toString: String =
    s"AdaptivePrediction(" +
      s"windowSizeMs=$windowSizeMs, " +
      s"watermarkDelayMs=$watermarkDelayMs, " +
      s"confidence=$confidence, " +
      s"strategy=$strategy, " +
      s"source=$source, " +
      s"modelVersion=$modelVersion, " +
      s"inferenceLatencyMs=$inferenceLatencyMs, " +
      s"timestamp=$timestamp)"
}

object AdaptivePrediction {

  /**
   * Safe fallback prediction.
   *
   * IMPORTANT:
   * - MUST NOT pretend to be ONNX
   * - Must explicitly mark RuleBased origin
   */
  def empty(timestamp: Long = System.currentTimeMillis()): AdaptivePrediction =
    AdaptivePrediction(
      windowSizeMs = 5000L,
      watermarkDelayMs = 3000L,
      confidence = 0.0,
      strategy = DecisionStrategy.RuleBased,
      source = PredictionSource.RuleBased,
      modelVersion = Some("fallback_v1"),
      inferenceLatencyMs = Some(0.0),
      timestamp = timestamp
    )

  /**
   * Factory for ONNX predictions.
   */
  def fromONNX(
    window: Long,
    watermark: Long,
    confidence: Double,
    modelVersion: String,
    latencyMs: Double
  ): AdaptivePrediction =
    AdaptivePrediction(
      windowSizeMs = window,
      watermarkDelayMs = watermark,
      confidence = confidence,
      strategy = DecisionStrategy.ONNX,
      source = PredictionSource.ONNX,
      modelVersion = Some(modelVersion),
      inferenceLatencyMs = Some(latencyMs)
    )
}

/**
 * Explicit provenance of prediction.
 */
sealed trait PredictionSource
object PredictionSource {
  case object ONNX extends PredictionSource
  case object RuleBased extends PredictionSource
  case object Hybrid extends PredictionSource
}
