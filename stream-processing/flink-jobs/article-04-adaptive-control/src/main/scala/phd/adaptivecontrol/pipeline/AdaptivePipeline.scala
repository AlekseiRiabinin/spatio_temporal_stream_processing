package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction

import phd.adaptivecontrol.config.AdaptiveConfig
import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.interaction.InteractionEngine
import phd.adaptivecontrol.adaptive.{
  StreamProfiler,
  AdaptiveController,
  ONNXInference,
  FeaturePreprocessor,
  AdaptiveRuntimeState
}


object AdaptivePipeline {

  // ============================================================
  // Interaction processor
  // ============================================================
  class InteractionFlatMap(config: AdaptiveConfig)
    extends FlatMapFunction[List[GeoEvent], Interaction]
    with Serializable {

    @transient private var engine: InteractionEngine = _
    @transient private var initialized: Boolean = false
    @transient private var lastAdaptationTs: Long = 0L

    // ============================================================
    // Initialization
    // ============================================================
    private def initIfNeeded(): Unit = {

      if (!initialized) {

        StreamProfiler.setConfig(config)

        // ----------------------------------------------------------
        // Runtime mode initialization (FIXED vs ADAPTIVE)
        // ----------------------------------------------------------
        if (config.isAdaptive) {
          AdaptiveRuntimeState.setMode(AdaptiveRuntimeState.Adaptive)
        } else {
          AdaptiveRuntimeState.setMode(AdaptiveRuntimeState.Fixed)
        }

        println(
          s"[ADAPTIVE CONTROL] action=runtime_init " +
          s"mode=${AdaptiveRuntimeState.currentMode} " +
          s"isAdaptive=${config.isAdaptive} " +
          s"mlInference=${config.mlInference}"
        )

        // ----------------------------------------------------------
        // ML stack initialization
        // ----------------------------------------------------------
        if (config.mlInference) {

          println("[ADAPTIVE CONTROL] action=preprocessor_initialize")
          FeaturePreprocessor.initialize(config)

          println(
            s"[ADAPTIVE CONTROL] action=preprocessor_status " +
            s"initialized=${FeaturePreprocessor.isInitialized}"
          )

          println("[ADAPTIVE CONTROL] action=onnx_initialize")
          ONNXInference.initialize(config)

          println(
            s"[ADAPTIVE CONTROL] action=onnx_status " +
            s"status=${ONNXInference.status}"
          )
        }

        initialized = true
      }
    }

    private def getEngine: InteractionEngine = {
      if (engine == null)
        engine = new InteractionEngine(config)

      engine
    }

    private def shouldAdapt(): Boolean = {
      val now = System.currentTimeMillis()

      if (now - lastAdaptationTs >= config.adaptationIntervalMs) {
        lastAdaptationTs = now
        true
      } else {
        false
      }
    }

    // ============================================================
    // Main processing
    // ============================================================
    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      initIfNeeded()

      val t0 = System.currentTimeMillis()

      // ----------------------------------------------------------
      // 1. Profiling (events first)
      // ----------------------------------------------------------
      StreamProfiler.updateEvents(batch)
      StreamProfiler.updateWindow(batch.size)

      // ----------------------------------------------------------
      // 2. Interaction processing
      // ----------------------------------------------------------
      val results =
        getEngine.process(batch)

      StreamProfiler.updateInteractions(results)

      // ----------------------------------------------------------
      // 3. Watermark
      // ----------------------------------------------------------
      val now = System.currentTimeMillis()

      val wmDelay =
        if (AdaptiveRuntimeState.isAdaptive)
          AdaptiveRuntimeState.watermarkDelayMs
        else
          config.watermarkDelayMs

      val computedWatermark = now - wmDelay

      StreamProfiler.updateWatermark(computedWatermark)

      // ----------------------------------------------------------
      // 4. Snapshot after full state update
      // ----------------------------------------------------------
      val features =
        StreamProfiler.snapshot()

      // ============================================================
      // 5. Adaptive control logic
      // ============================================================
      val adaptiveEnabled =
        AdaptiveRuntimeState.isAdaptive && config.mlInference

      if (adaptiveEnabled && shouldAdapt()) {

        if (!FeaturePreprocessor.isInitialized) {
          throw new IllegalStateException(
            "FeaturePreprocessor is not initialized"
          )
        }

        val prediction =
          ONNXInference.predict(features)

        val decision =
          AdaptiveController.decide(features, prediction)

        AdaptiveRuntimeState.update(
          decision.windowSizeMs,
          decision.watermarkDelayMs
        )

        println(
          "[ADAPTIVE CONTROL] action=config_update " +
          s"window=${decision.windowSizeMs} " +
          s"watermark=${decision.watermarkDelayMs}"
        )

        StreamProfiler.updateAdaptiveDecision(
          decision,
          prediction.inferenceLatencyMs.getOrElse(0.0)
        )

        println(
          "[ADAPTIVE CONTROL] " +
          s"window=${decision.windowSizeMs} " +
          s"watermark=${decision.watermarkDelayMs} " +
          s"inference_ms=${prediction.inferenceLatencyMs.getOrElse(0.0).formatted("%.3f")} " +
          s"confidence=${decision.confidence.formatted("%.4f")} " +
          s"strategy=${decision.strategy} " +
          s"model_version=${decision.modelVersion.getOrElse("none")}"
        )
      }

      val t1 = System.currentTimeMillis()

      StreamProfiler.recordProcessingLatency(t1 - t0)

      StreamProfiler.logSnapshot()

      results.foreach(out.collect)
    }
  }

  // ============================================================
  // Pipeline build
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
      s"windowMode=${config.windowMode} " +
      s"watermarkMode=${config.watermarkMode} " +
      s"mlInference=${config.mlInference} " +
      s"adaptationIntervalMs=${config.adaptationIntervalMs}"
    )

    val windowedStream =
      AdaptiveWindowOperator(inputStream, config)

    println(
      "[ADAPTIVE PIPELINE] action=windowing status=initialized"
    )

    implicit val interactionTypeInfo: TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    val interactions =
      windowedStream.flatMap(new InteractionFlatMap(config))

    println(
      "[ADAPTIVE PIPELINE] action=interactionAnalysis status=ready"
    )

    println(
      "[ADAPTIVE PIPELINE] action=adaptiveControl status=enabled"
    )

    interactions
  }
}
