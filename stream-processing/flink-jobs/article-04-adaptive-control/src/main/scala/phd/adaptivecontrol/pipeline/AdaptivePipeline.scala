package phd.adaptivecontrol.pipeline

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction

import phd.adaptivecontrol.model.{GeoEvent, Interaction}
import phd.adaptivecontrol.interaction.InteractionEngine
import phd.adaptivecontrol.adaptive.StreamProfiler


object AdaptivePipeline {

  // ============================================================
  // Interaction processing (Flink-safe)
  // ============================================================
  class InteractionFlatMap
    extends FlatMapFunction[List[GeoEvent], Interaction]
    with Serializable {

    @transient private var engine: InteractionEngine = _

    private def getEngine: InteractionEngine = {
      if (engine == null) engine = new InteractionEngine()
      engine
    }

    override def flatMap(
      batch: List[GeoEvent],
      out: Collector[Interaction]
    ): Unit = {

      val results = getEngine.process(batch)

      // ✅ interaction-level profiling
      StreamProfiler.updateInteractions(results)

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
    // 1. Event-level profiling
    // ------------------------------------------------------------
    val profiledEvents =
      inputStream.map { event =>
        StreamProfiler.updateEvents(Seq(event))
        event
      }

    // ------------------------------------------------------------
    // 2. Window processing
    // ------------------------------------------------------------
    val windowedStream: DataStream[List[GeoEvent]] =
      WindowProcessor.applyWindow(profiledEvents)

    println("[ADAPTIVE PIPELINE] action=windowing status=initialized")

    // ------------------------------------------------------------
    // 3. Window-level profiling (IMPORTANT FOR ML FEATURES)
    // ------------------------------------------------------------
    val profiledWindows =
      windowedStream.map { batch =>
        StreamProfiler.updateEvents(batch)
        StreamProfiler.updateWindow(batch.size)
        batch
      }

    // ------------------------------------------------------------
    // 4. Interaction processing
    // ------------------------------------------------------------
    implicit val interactionTypeInfo: TypeInformation[Interaction] =
      createTypeInformation[Interaction]

    val interactions =
      profiledWindows.flatMap(new InteractionFlatMap)

    println("[ADAPTIVE PIPELINE] action=interactionAnalysis status=ready")

    // ------------------------------------------------------------
    // 5. Snapshot emission (ML dataset hook)
    // ------------------------------------------------------------
    val profiledInteractions =
      interactions.map { i =>
        StreamProfiler.logSnapshot()
        i
      }

    profiledInteractions
  }
}
