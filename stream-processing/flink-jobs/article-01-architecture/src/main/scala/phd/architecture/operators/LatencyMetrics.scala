package phd.architecture.operators

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import phd.architecture.model.WindowResult
import phd.architecture.model.TypeInfos._


final class AttachProcessingLatencyFunction
  extends ProcessFunction[WindowResult, WindowResult] {

  override def processElement(
    value: WindowResult,
    ctx: ProcessFunction[WindowResult, WindowResult]#Context,
    out: Collector[WindowResult]
  ): Unit = {

    // current processing time (system time)
    val now = ctx.timerService().currentProcessingTime()

    // compute latency using windowEnd as event-time boundary
    val eventTs = value.windowEnd
    val latency = now - eventTs

    // print latency log for the experimental section
    println(
      s"[process-latency] partition=${value.partition.geohash} " +
      s"windowEnd=$eventTs processingTs=$now latencyMs=$latency"
    )

    // attach processing time to the result
    val updated = value.copy(processingTime = Some(now))
    out.collect(updated)
  }
}

object LatencyMetrics {

  /**
    * Attaches processing-time to each WindowResult
    * and prints latency logs.
    */
  def attachProcessingLatency(
    stream: DataStream[WindowResult]
  ): DataStream[WindowResult] = {

    val fn = new AttachProcessingLatencyFunction
    stream.process(fn)
  }
}


// docker logs -f flink-taskmanager
