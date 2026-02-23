package phd.streammodels.stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.algorithms.StreamModel
import phd.streammodels.windows.WindowStrategy
import phd.streammodels.model.TypeInfos._


object StreamTopology {

  /**
    * Build the full pipeline:
    *   1. Read events from Kafka
    *   2. Apply the selected stream model (Actor, Log, Dataflow, MicroBatch)
    *   3. Apply the selected window strategy (Session, Dynamic, Adaptive, MultiTrigger)
    */
  def build[K](
    env: StreamExecutionEnvironment,
    model: StreamModel[K],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    val source: DataStream[Event] =
      KafkaSourceFactory.createEventSource(env)

    model.buildPipeline(source, windowStrategy)
  }

  /**
    * Simple sink for debugging Article 2 experiments.
    */
  def sinkResults[K: TypeInformation](
    results: DataStream[WindowResult[K]]
  ): DataStream[WindowResult[K]] = {

    val debugged = results.map { r: WindowResult[K] =>
      println(
        s"📊 RESULT: key=${r.partition}, " +
        s"window=[${r.windowStart}, ${r.windowEnd}], " +
        s"value=${r.value}, " +
        s"processingTime=${r.processingTime.getOrElse(-1L)}"
      )
      r
    }.name("DebugSink")

    debugged.print().name("PrintSink")

    debugged
  }
}
