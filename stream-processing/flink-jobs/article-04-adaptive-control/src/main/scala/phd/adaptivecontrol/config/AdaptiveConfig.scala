package phd.adaptivecontrol.config


case class AdaptiveConfig(

  // ------------------------------------------------------------
  // Fixed (configured) values
  // ------------------------------------------------------------
  windowSizeMs: Long,
  watermarkDelayMs: Long,

  // ------------------------------------------------------------
  // Adaptive (dynamic) values
  // ------------------------------------------------------------
  var adaptiveWindowSizeMs: Long = 0L,
  var adaptiveWatermarkDelayMs: Long = 0L,

  // ------------------------------------------------------------
  // Strategies
  // ------------------------------------------------------------
  windowStrategy: String = "fixed",
  watermarkStrategy: String = "fixed",

  // ------------------------------------------------------------
  // ML inference
  // ------------------------------------------------------------
  mlInference: Boolean = false,

  // ------------------------------------------------------------
  // ONNX models
  // ------------------------------------------------------------
  windowModelPath: String = "/opt/models/model_a_window.onnx",
  watermarkModelPath: String = "/opt/models/model_b_watermark.onnx",

  // ------------------------------------------------------------
  // Adaptation interval
  // ------------------------------------------------------------
  adaptationIntervalMs: Long = 2000L
)
