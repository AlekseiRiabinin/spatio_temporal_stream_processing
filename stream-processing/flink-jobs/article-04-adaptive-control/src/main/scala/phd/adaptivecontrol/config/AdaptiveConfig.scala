package phd.adaptivecontrol.config


/**
 * AdaptiveConfig
 *
 * Clean configuration model:
 * - NO string-based mode switching
 * - Explicit typed strategy modes
 */
case class AdaptiveConfig(

  // ------------------------------------------------------------
  // Fixed (configured) baseline values
  // ------------------------------------------------------------
  windowSizeMs: Long,
  watermarkDelayMs: Long,

  // ------------------------------------------------------------
  // Adaptive (runtime-updated values)
  // ------------------------------------------------------------
  @volatile var adaptiveWindowSizeMs: Long = 0L,
  @volatile var adaptiveWatermarkDelayMs: Long = 0L,

  // ------------------------------------------------------------
  // Strategy modes
  // ------------------------------------------------------------
  windowMode: StrategyMode = StrategyMode.Fixed,
  watermarkMode: StrategyMode = StrategyMode.Fixed,

  // ------------------------------------------------------------
  // ML inference
  // ------------------------------------------------------------
  mlInference: Boolean = false,

  // ------------------------------------------------------------
  // ONNX models
  // ------------------------------------------------------------
  windowModelPath: String = "/opt/models/model_a_window.onnx",
  watermarkModelPath: String = "/opt/models/model_b_watermark.onnx",
  scalerParamsPath: String = "/opt/models/scaler_params.json",

  // ------------------------------------------------------------
  // Adaptation interval
  // ------------------------------------------------------------
  adaptationIntervalMs: Long = 2000L
) {

  /**
   * Convenience helper used across pipeline.
   * Eliminates repeated OR logic like:
   * windowStrategy == "adaptive" || watermarkStrategy == "adaptive"
   */
  def isAdaptive: Boolean =
    windowMode == StrategyMode.Adaptive ||
    watermarkMode == StrategyMode.Adaptive

  def isFixed: Boolean =
    !isAdaptive
}
