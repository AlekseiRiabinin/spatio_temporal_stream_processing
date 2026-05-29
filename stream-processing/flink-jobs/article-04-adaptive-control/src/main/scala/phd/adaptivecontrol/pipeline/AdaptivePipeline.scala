package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction

import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.interaction.InteractionEngine


/**
 * AdaptivePipeline
 *
 * High-level orchestration layer for Article 04.
 *
 * Responsibilities:
 *   - window control
 *   - interaction analysis
 *   - runtime stream processing
 *
 * NOTE:
 * Watermarking is intentionally NOT handled here.
 * It is managed at job level (Article04AdaptiveControlJob).
 */
object AdaptivePipeline {

  // ============================================================
  // FlatMap Function (Flink-safe, lazy initialization)
  // ============================================================
  class InteractionFlatMap
    extends FlatMapFunction[List[GeoEvent], Interaction]
    with Serializable {

    @transient private var engine: InteractionEngine = _

    private def getEngine: InteractionEngine = {
      if (engine == null) { engine = new InteractionEngine() }
      engine
    }

    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      val results = getEngine.process(batch)
      results.foreach(out.collect)
    }
  }

  // ============================================================
  // Pipeline
  // ============================================================
    def build(
      env: StreamExecutionEnvironment,
      inputStream: DataStream[GeoEvent],
      windowSizeMs: Long
    ): DataStream[Interaction] = {

    println("[ADAPTIVE PIPELINE] action=start")

    // ------------------------------------------------------------
    // 1. Window processing (already event-time aware stream)
    // ------------------------------------------------------------
    val windowedStream: DataStream[List[GeoEvent]] =
      WindowProcessor.applyWindow(inputStream)

    println("[ADAPTIVE PIPELINE] action=windowing status=initialized")

    // ------------------------------------------------------------
    // 2. Interaction analysis
    // ------------------------------------------------------------
    implicit val interactionTypeInfo: TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    val interactions =
      windowedStream.flatMap(new InteractionFlatMap)

    println("[ADAPTIVE PIPELINE] action=interactionAnalysis status=ready")

    // ------------------------------------------------------------
    // 3. Future adaptive-control hooks
    // ------------------------------------------------------------
    println(
      "[ADAPTIVE PIPELINE] action=adaptiveControl status=disabled"
    )

    // Future adaptive workflow:
    //
    // val profile =
    //   StreamProfiler.profile(...)
    //
    // val features =
    //   FeatureExtractor.extract(profile)
    //
    // val decision =
    //   ONNXInference.predict(features)
    //
    // AdaptiveController.apply(
    //   decision,
    //   env
    // )
    //

    interactions
  }
}
