package phd.streammodels.stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import phd.streammodels.model.{Event, WindowResult}
import phd.streammodels.algorithms.StreamModel
import phd.streammodels.windows.WindowStrategy
import phd.streammodels.model.TypeInfos._


object StreamTopology {

  def build[K](
    env: StreamExecutionEnvironment,
    model: StreamModel[K],
    windowStrategy: WindowStrategy[K]
  ): DataStream[WindowResult[K]] = {

    println(
      s"""
        |Building pipeline →
        |  model  = ${model.getClass.getSimpleName}
        |  window = ${windowStrategy.getClass.getSimpleName}
        |""".stripMargin
    )

    val source: DataStream[Event] =
      KafkaSourceFactory.createEventSource(env)

    println("Source initialized")

    val pipeline = model.buildPipeline(source, windowStrategy)

    println(s"Pipeline created → type=${pipeline.getClass.getSimpleName}")

    pipeline
  }

  def sinkResults[K: TypeInformation](
    results: DataStream[WindowResult[K]]
  ): DataStream[WindowResult[K]] = {

    val debugged = 
      results
        .map { r: WindowResult[K] => r }
        .name("TopologyDebugSink")

    debugged
  }
}
