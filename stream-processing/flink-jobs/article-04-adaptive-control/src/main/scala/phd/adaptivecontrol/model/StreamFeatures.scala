package phd.adaptivecontrol.model

/**
 * StreamFeatures
 *
 * Clean ML feature vector for:
 *   - ONNX inference
 *   - adaptive window control
 *   - watermark tuning
 *   - spatio-temporal behavior modeling
 */
case class StreamFeatures(

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
  // Metadata
  // ============================================================
  timestamp: Long
) {

  // ============================================================
  // ONNX input vector
  // ============================================================
  def toVector: Array[Float] = {
    Array(
      eventRate.toFloat,
      disorderRatio.toFloat,
      lateEventRatio.toFloat,
      averageLatencyMs.toFloat,

      windowFillRatio.toFloat,

      interactionRate.toFloat,
      collisionRate.toFloat,
      proximityRate.toFloat,
      swarmRate.toFloat,
      conflictRate.toFloat,

      watermarkLagMs.toFloat,
      processingLatencyMs.toFloat
    )
  }

  // ============================================================
  // Simple ML heuristics
  // ============================================================
  def isHighlyDisordered: Boolean =
    disorderRatio > 0.3 || lateEventRatio > 0.2

  def isHighLoad: Boolean =
    eventRate > 100.0 || processingLatencyMs > 1000.0

  override def toString: String =
    s"StreamFeatures(" +
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
      s"timestamp=$timestamp)"
  }
