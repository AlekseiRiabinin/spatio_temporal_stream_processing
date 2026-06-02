package phd.adaptivecontrol.config


/**
 * AdaptiveConfig
 *
 * Unified configuration for:
 *   - fixed windowing
 *   - fixed watermarking
 *   - adaptive window control
 *   - adaptive watermark tuning
 *   - ML inference hooks
 */
case class AdaptiveConfig(

  // ------------------------------------------------------------
  // Fixed (configured) values
  // ------------------------------------------------------------
  windowSizeMs: Long,
  watermarkDelayMs: Long,

  // ------------------------------------------------------------
  // Adaptive (dynamic) values — must be mutable
  // ------------------------------------------------------------
  var adaptiveWindowSizeMs: Long = 0L,
  var adaptiveWatermarkDelayMs: Long = 0L,

  // ------------------------------------------------------------
  // Strategies (future-proof)
  // ------------------------------------------------------------
  windowStrategy: String = "fixed",        // fixed | adaptive
  watermarkStrategy: String = "fixed",     // fixed | adaptive

  // ------------------------------------------------------------
  // ML inference
  // ------------------------------------------------------------
  mlInference: Boolean = false,

  // ------------------------------------------------------------
  // Adaptation interval (ms)
  // ------------------------------------------------------------
  adaptationIntervalMs: Long = 2000L
)
