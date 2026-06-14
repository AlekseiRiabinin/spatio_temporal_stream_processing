package phd.adaptivecontrol.model


/**
 * StreamFeatures
 *
 * Complete feature snapshot used by:
 *   - StreamProfiler
 *   - FeatureExtractor
 *   - FeaturePreprocessor
 *   - ONNXInference
 */
case class StreamFeatures(

  // ============================================================
  // Categorical ML features
  // ============================================================
  profile: String,
  ratePattern: String,
  motionMode: String,

  // ============================================================
  // Temporal stream behavior
  // ============================================================
  eventRate: Double,
  disorderRatio: Double,
  lateEventRatio: Double,
  averageLatencyMs: Double,

  // ============================================================
  // Window behavior
  // ============================================================
  windowFillRatio: Double,

  // ============================================================
  // Interaction dynamics
  // ============================================================
  interactionRate: Double,
  collisionRate: Double,
  proximityRate: Double,
  swarmRate: Double,
  conflictRate: Double,

  // ============================================================
  // System state
  // ============================================================
  watermarkLagMs: Long,
  processingLatencyMs: Double,

  // ============================================================
  // Adaptive control values
  // ============================================================
  adaptiveWindowSizeMs: Long,
  adaptiveWatermarkDelayMs: Long,

  // ============================================================
  // Metadata
  // ============================================================
  timestamp: Long
) {

  // ============================================================
  // Simple ML heuristics
  // ============================================================

  def isHighlyDisordered: Boolean =
    disorderRatio > 0.3 || lateEventRatio > 0.2

  def isHighLoad: Boolean =
    eventRate > 100.0 || processingLatencyMs > 1000.0

  override def toString: String =
    s"StreamFeatures(" +
      s"profile=$profile, " +
      s"ratePattern=$ratePattern, " +
      s"motionMode=$motionMode, " +
      s"eventRate=$eventRate, " +
      s"disorderRatio=$disorderRatio, " +
      s"lateEventRatio=$lateEventRatio, " +
      s"avgLatencyMs=$averageLatencyMs, " +
      s"windowFillRatio=$windowFillRatio, " +
      s"interactionRate=$interactionRate, " +
      s"collisionRate=$collisionRate, " +
      s"proximityRate=$proximityRate, " +
      s"swarmRate=$swarmRate, " +
      s"conflictRate=$conflictRate, " +
      s"watermarkLagMs=$watermarkLagMs, " +
      s"processingLatencyMs=$processingLatencyMs, " +
      s"adaptiveWindowSizeMs=$adaptiveWindowSizeMs, " +
      s"adaptiveWatermarkDelayMs=$adaptiveWatermarkDelayMs, " +
      s"timestamp=$timestamp)"
}

object StreamFeatures {

  def empty(
    timestamp: Long = System.currentTimeMillis()
  ): StreamFeatures =
    StreamFeatures(

      profile = "realtime",
      ratePattern = "constant",
      motionMode = "straight",

      eventRate = 0.0,
      disorderRatio = 0.0,
      lateEventRatio = 0.0,
      averageLatencyMs = 0.0,

      windowFillRatio = 0.0,

      interactionRate = 0.0,
      collisionRate = 0.0,
      proximityRate = 0.0,
      swarmRate = 0.0,
      conflictRate = 0.0,

      watermarkLagMs = 0L,
      processingLatencyMs = 0.0,

      adaptiveWindowSizeMs = 0L,
      adaptiveWatermarkDelayMs = 0L,

      timestamp = timestamp
    )
}
