package phd.adaptivecontrol.model


/**
 * StreamFeatures
 *
 * Runtime stream characteristics extracted from
 * incoming spatio-temporal event streams.
 *
 * Used for:
 *   - adaptive watermark control
 *   - adaptive window sizing
 *   - disorder analysis
 *   - ML inference
 *   - runtime optimization
 *
 * IMPORTANT:
 * Features are computed continuously during stream execution.
 */
case class StreamFeatures(
  eventRate: Double,              // events/sec
  outOfOrderRatio: Double,        // 0.0 .. 1.0
  averageDelayMs: Double,
  maxDelayMs: Long,
  averageSpeed: Double,
  density: Double,
  windowEventCount: Long,
  watermarkLagMs: Long,
  processingLatencyMs: Double,
  timestamp: Long
) {

  /**
   * Convert features into vector representation
   * suitable for ONNX inference.
   */
  def toVector: Array[Float] = {

    Array(
      eventRate.toFloat,
      outOfOrderRatio.toFloat,
      averageDelayMs.toFloat,
      maxDelayMs.toFloat,
      averageSpeed.toFloat,
      density.toFloat,
      windowEventCount.toFloat,
      watermarkLagMs.toFloat,
      processingLatencyMs.toFloat
    )
  }

  /**
   * Simple diagnostics.
   */
  def isHighlyDisordered: Boolean = {
    outOfOrderRatio > 0.3 || averageDelayMs > 5000
  }

  /**
   * Human-readable representation.
   */
  override def toString: String = {

    s"StreamFeatures(" +
      s"eventRate=$eventRate, " +
      s"outOfOrderRatio=$outOfOrderRatio, " +
      s"averageDelayMs=$averageDelayMs, " +
      s"maxDelayMs=$maxDelayMs, " +
      s"averageSpeed=$averageSpeed, " +
      s"density=$density, " +
      s"windowEventCount=$windowEventCount, " +
      s"watermarkLagMs=$watermarkLagMs, " +
      s"processingLatencyMs=$processingLatencyMs, " +
      s"timestamp=$timestamp)"
  }
}
