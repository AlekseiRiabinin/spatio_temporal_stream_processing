package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.interaction.InteractionEngine
import phd.adaptivecontrol.adaptive.{StreamProfiler, AdaptiveController}


object AdaptivePipeline {

  // ============================================================
  // Interaction processing + profiling (Flink-safe)
  // ============================================================
  class InteractionFlatMap(config: AdaptiveConfig)
    extends FlatMapFunction[List[GeoEvent], Interaction]
    with Serializable {

    @transient private var engine: InteractionEngine = _
    @transient private var initialized: Boolean = false

    /** Initialize profiler on the TaskManager JVM */
    private def initIfNeeded(): Unit = {
      if (!initialized) {
        StreamProfiler.setConfig(config)
        initialized = true
      }
    }

    private def getEngine: InteractionEngine = {
      if (engine == null) {
        engine = new InteractionEngine(config)
      }
      engine
    }

    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      // ----------------------------------------------------------
      // 0. TM-side initialization
      // ----------------------------------------------------------
      initIfNeeded()

      // ----------------------------------------------------------
      // 1. Event profiling
      // ----------------------------------------------------------
      StreamProfiler.updateEvents(batch)

      // ----------------------------------------------------------
      // 2. Interaction analysis
      // ----------------------------------------------------------
      val results = getEngine.process(batch)

      // ----------------------------------------------------------
      // 3. Interaction profiling
      // ----------------------------------------------------------
      StreamProfiler.updateInteractions(results)

      // ----------------------------------------------------------
      // 4. Snapshot (features)
      // ----------------------------------------------------------
      val features = StreamProfiler.snapshot()

      // ----------------------------------------------------------
      // 5. Adaptive control (if enabled)
      // ----------------------------------------------------------
      if (config.windowStrategy == "adaptive" ||
          config.watermarkStrategy == "adaptive" ||
          config.mlInference) {

        val decision = AdaptiveController.decide(features)

        // Update adaptive config values
        config.adaptiveWindowSizeMs = decision.windowSizeMs
        config.adaptiveWatermarkDelayMs = decision.watermarkDelayMs

        println(
          s"[ADAPTIVE CONTROL] decision=" +
          s"window=${decision.windowSizeMs} " +
          s"watermark=${decision.watermarkDelayMs} " +
          s"confidence=${decision.confidence} " +
          s"strategy=${decision.strategy}"
        )
      }

      // ----------------------------------------------------------
      // 6. Log metrics
      // ----------------------------------------------------------
      StreamProfiler.logSnapshot()

      // ----------------------------------------------------------
      // 7. Emit interactions
      // ----------------------------------------------------------
      results.foreach(out.collect)
    }
  }

  // ============================================================
  // Pipeline
  // ============================================================
  def build(
    env: StreamExecutionEnvironment,
    inputStream: DataStream[GeoEvent],
    config: AdaptiveConfig
  ): DataStream[Interaction] = {

    println(
      "[ADAPTIVE PIPELINE] action=start " +
      s"windowSizeMs=${config.windowSizeMs} " +
      s"watermarkDelayMs=${config.watermarkDelayMs} " +
      s"windowStrategy=${config.windowStrategy} " +
      s"watermarkStrategy=${config.watermarkStrategy} " +
      s"mlInference=${config.mlInference} " +
      s"adaptationIntervalMs=${config.adaptationIntervalMs}"
    )

    // ------------------------------------------------------------
    // 1. Window processing
    // ------------------------------------------------------------
    val windowedStream: DataStream[List[GeoEvent]] =
      WindowProcessor.applyWindow(inputStream, config)

    println("[ADAPTIVE PIPELINE] action=windowing status=initialized")

    // ------------------------------------------------------------
    // 2. Interaction analysis + profiling + adaptive control
    // ------------------------------------------------------------
    implicit val interactionTypeInfo: TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    val interactions: DataStream[Interaction] =
      windowedStream.flatMap(new InteractionFlatMap(config))

    println("[ADAPTIVE PIPELINE] action=interactionAnalysis status=ready")
    println("[ADAPTIVE PIPELINE] action=adaptiveControl status=enabled")

    interactions
  }
}
