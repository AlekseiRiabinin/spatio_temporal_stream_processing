package phd.adaptivecontrol.adaptive

import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.StreamFeatures


object FeaturePreprocessor extends Serializable {

  // ============================================================
  // JSON model
  // ============================================================

  case class ScalerParams(
    numeric_features: List[String],
    mean: List[Double],
    scale: List[Double]
  )

  // ============================================================
  // Categories from feature_schema.py
  // ============================================================

  private val Profiles = Seq(
    "realtime",
    "skewed",
    "late",
    "out_of_order",
    "mixed"
  )

  private val RatePatterns = Seq(
    "constant",
    "burst",
    "wave"
  )

  private val MotionModes = Seq(
    "straight",
    "corridor",
    "swarm",
    "collision",
    "random_walk"
  )

  // ============================================================
  // Runtime state
  // ============================================================

  @volatile
  private var initialized = false

  private var means: Array[Double] = _
  private var scales: Array[Double] = _

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  // ============================================================
  // Initialization
  // ============================================================

  def initialize(config: AdaptiveConfig): Unit = synchronized {

    if (initialized)
      return

    println(
      s"[PREPROCESSOR] action=load path=${config.scalerParamsPath}"
    )

    val json =
      Source
        .fromFile(config.scalerParamsPath)
        .mkString

    val params =
      mapper.readValue(json, classOf[ScalerParams])

    means = params.mean.toArray
    scales = params.scale.toArray

    initialized = true

    println(
      "[PREPROCESSOR] action=ready " +
      s"numericFeatures=${means.length}"
    )
  }

  // ============================================================
  // Main transform
  // ============================================================

  def transform(features: StreamFeatures): Array[Float] = {

    if (!initialized) {
      throw new IllegalStateException("FeaturePreprocessor not initialized")
    }

    val categorical =
      encodeCategorical(features)

    val numeric =
      scaleNumeric(features)

    val result =
      categorical ++ numeric

    if (result.length != 25) {
      throw new IllegalStateException(
        s"Expected 25 ONNX features, got ${result.length}"
      )
    }

    result
  }

  // ============================================================
  // One-hot encoding
  // ============================================================

  private def encodeCategorical(f: StreamFeatures): Array[Float] = {

    val profileVector =
      Profiles.map {
        value =>
          if (value == f.profile) 1.0f
          else 0.0f
      }

    val ratePatternVector =
      RatePatterns.map {
        value =>
          if (value == f.ratePattern) 1.0f
          else 0.0f
      }

    val motionModeVector =
      MotionModes.map {
        value =>
          if (value == f.motionMode) 1.0f
          else 0.0f
      }

    (
      profileVector ++
      ratePatternVector ++
      motionModeVector
    ).toArray
  }

  // ============================================================
  // StandardScaler transform
  // ============================================================

  private def scaleNumeric(f: StreamFeatures): Array[Float] = {

    val raw = Array[Double](

      f.eventRate,
      f.disorderRatio,
      f.lateEventRatio,
      f.averageLatencyMs,

      f.windowFillRatio,

      f.interactionRate,
      f.collisionRate,
      f.proximityRate,
      f.swarmRate,
      f.conflictRate,

      f.watermarkLagMs.toDouble,
      f.processingLatencyMs
    )

    raw.indices.map { i =>

      val mean =
        means(i)

      val scale =
        if (scales(i) == 0.0) 1.0
        else scales(i)

      ((raw(i) - mean) / scale).toFloat

    }.toArray
  }

  // ============================================================
  // Metadata
  // ============================================================

  def outputDimension: Int =
    Profiles.size +
    RatePatterns.size +
    MotionModes.size +
    12

  def isInitialized: Boolean =
    initialized
}
