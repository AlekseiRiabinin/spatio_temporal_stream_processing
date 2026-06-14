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
  ONNXInference
}


object AdaptivePipeline {

  class InteractionFlatMap(config: AdaptiveConfig)
    extends FlatMapFunction[List[GeoEvent], Interaction]
    with Serializable {

    @transient private var engine: InteractionEngine = _
    @transient private var initialized: Boolean = false
    @transient private var lastAdaptationTs: Long = 0L

    private def initIfNeeded(): Unit = {

      if (!initialized) {

        StreamProfiler.setConfig(config)

        // ------------------------------------------------------
        // Initialize ONNX inside TaskManager JVM
        // ------------------------------------------------------
        if (config.mlInference) {

          println(
            "[ADAPTIVE CONTROL] action=onnx_initialize"
          )

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

      if (
        now - lastAdaptationTs >=
        config.adaptationIntervalMs
      ) {
        lastAdaptationTs = now
        true
      } else {
        false
      }
    }

    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      initIfNeeded()

      // ----------------------------------------------------------
      // Profiling
      // ----------------------------------------------------------

      StreamProfiler.updateEvents(batch)

      val results =
        getEngine.process(batch)

      StreamProfiler.updateInteractions(results)

      StreamProfiler.updateWindow(batch.size)

      val features =
        StreamProfiler.snapshot()

      // ----------------------------------------------------------
      // Adaptive mode only
      // ----------------------------------------------------------

      val adaptiveEnabled =
        config.windowStrategy == "adaptive" ||
        config.watermarkStrategy == "adaptive"

      if (adaptiveEnabled && shouldAdapt()) {

        val prediction =
          ONNXInference.predict(features)

        val decision =
          AdaptiveController.decide(
            features,
            prediction
          )

        // ----------------------------------------
        // Update runtime configuration
        // ----------------------------------------

        config.adaptiveWindowSizeMs =
          decision.windowSizeMs

        config.adaptiveWatermarkDelayMs =
          decision.watermarkDelayMs

        StreamProfiler.updateAdaptiveDecision(
          decision,
          prediction.inferenceLatencyMs.getOrElse(0.0)
        )

        println(
          "[ADAPTIVE CONTROL] " +
          s"window=${decision.windowSizeMs} " +
          s"watermark=${decision.watermarkDelayMs} " +
          s"inference_ms=${
            prediction.inferenceLatencyMs
              .getOrElse(0.0)
              .formatted("%.3f")
          } " +
          s"confidence=${decision.confidence.formatted("%.4f")} " +
          s"strategy=${decision.strategy} " +
          s"model_version=${
            decision.modelVersion.getOrElse("none")
          }"
        )
      }

      StreamProfiler.logSnapshot()

      results.foreach(out.collect)
    }
  }

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

    val windowedStream =
      AdaptiveWindowOperator(
        inputStream,
        config
      )

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
