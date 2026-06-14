package phd.adaptivecontrol.adaptive

import phd.adaptivecontrol.model.StreamFeatures


object FeatureExtractor extends Serializable {

  // ============================================================
  // RAW TRAINING FEATURE ORDER
  // ============================================================

  private val FEATURE_ORDER: Array[String] = Array(

    // categorical
    "profile",
    "ratePattern",
    "motionMode",

    // numeric
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
    "processingLatencyMs"
  )

  // ============================================================
  // Snapshot
  // ============================================================

  def extract(): StreamFeatures =
    StreamProfiler.snapshot()

  // ============================================================
  // Raw feature snapshot
  // ============================================================

  def extractRawFeatures(): StreamFeatures =
    StreamProfiler.snapshot()

  // ============================================================
  // ONNX input vector
  // ============================================================

  def extractVector(): Array[Float] = {

    val features =
      StreamProfiler.snapshot()

    FeaturePreprocessor.transform(features)
  }

  // ============================================================
  // Validation
  // ============================================================

  def validateSchema(): Unit = {

    val expectedRawFeatures = 15

    if (FEATURE_ORDER.length != expectedRawFeatures) {
      throw new IllegalStateException(
        s"Feature schema mismatch: expected=$expectedRawFeatures actual=${FEATURE_ORDER.length}"
      )
    }
  }
}
