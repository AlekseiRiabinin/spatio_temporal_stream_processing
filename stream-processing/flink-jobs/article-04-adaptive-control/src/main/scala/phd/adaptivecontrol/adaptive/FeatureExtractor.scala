package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model.StreamFeatures


/**
 * FeatureExtractor
 *
 * STRICT contract between:
 *   StreamProfiler → ML features → ONNX models
 *
 * IMPORTANT:
 * - StreamFeatures may evolve (logging / research)
 * - ONNX vector MUST remain stable
 */
object FeatureExtractor extends Serializable {

  // ============================================================
  // STRICT FEATURE ORDER (MUST MATCH ONNX TRAINING)
  // ============================================================

  private val FEATURE_ORDER: Array[String] = Array(
    "eventRate",
    "disorderRatio",
    "lateEventRatio",
    "averageLatencyMs",
    "windowFillRatio",

    "interactionRate",
    "collisionRate",
    "proximityRate",
    "swarmRate",
    "conflictRate",

    "watermarkLagMs",
    "processingLatencyMs",

    "adaptiveWindowSizeMs",
    "adaptiveWatermarkDelayMs"
  )

  // ============================================================
  // Extract full feature object (safe, immutable snapshot)
  // ============================================================

  def extract(): StreamFeatures =
    StreamProfiler.snapshot()

  // ============================================================
  // ONNX VECTOR (STRICT CONTRACT)
  // ============================================================

  def extractVector(): Array[Float] = {

    val f = StreamProfiler.snapshot()

    val vec = Array(
      f.eventRate.toFloat,
      f.disorderRatio.toFloat,
      f.lateEventRatio.toFloat,
      f.averageLatencyMs.toFloat,

      f.windowFillRatio.toFloat,

      f.interactionRate.toFloat,
      f.collisionRate.toFloat,
      f.proximityRate.toFloat,
      f.swarmRate.toFloat,
      f.conflictRate.toFloat,

      f.watermarkLagMs.toFloat,
      f.processingLatencyMs.toFloat,

      f.adaptiveWindowSizeMs.toFloat,
      f.adaptiveWatermarkDelayMs.toFloat
    )

    // ========================================================
    // SAFETY CHECK (fails fast if schema drift occurs)
    // ========================================================

    if (vec.length != FEATURE_ORDER.length) {
      throw new IllegalStateException(
        s"Feature vector mismatch: expected ${FEATURE_ORDER.length}, got ${vec.length}"
      )
    }

    vec
  }
}
