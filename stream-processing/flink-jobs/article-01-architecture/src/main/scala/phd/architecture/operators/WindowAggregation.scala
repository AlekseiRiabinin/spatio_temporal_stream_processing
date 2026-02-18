package phd.architecture.operators

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.metrics.{Gauge, Histogram, HistogramStatistics}

import scala.collection.mutable

import phd.architecture.model.{Event, SpatialPartition, WindowResult}


/**
 * Ω_count(W, p) = |{e ∈ W(p,T)}|
 * Counts number of events in a window per spatial partition.
 * Emits Flink metrics (Prometheus-compatible) without modifying the job graph.
 */
final class CountEventsWindowFunction
  extends ProcessWindowFunction[Event, WindowResult, SpatialPartition, TimeWindow] {

  @transient private var windowLatency: Histogram = _
  @transient private var lastProcessingLatency: Long = 0L

  override def open(parameters: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup

    // Histogram for window latency (seconds)
    windowLatency = group.histogram("windowLatency", new CustomLatencyHistogram)

    // Gauge for processing latency (ms) — registered inline with explicit type parameters
    group.gauge[Long, Gauge[Long]](
      "processingLatency",
      new Gauge[Long] {
        override def getValue: Long = lastProcessingLatency
      }
    )
  }

  override def process(
    key: SpatialPartition,
    context: Context,
    elements: Iterable[Event],
    out: Collector[WindowResult]
  ): Unit = {

    val count = elements.size.toLong
    val start = context.window.getStart
    val end = context.window.getEnd

    val now = context.currentProcessingTime

    // Window latency (seconds)
    val latencySeconds = (now - end) / 1000.0
    windowLatency.update(latencySeconds.toLong)

    // Processing latency (ms)
    lastProcessingLatency = now - end

    out.collect(
      WindowResult(
        partition = key,
        windowStart = start,
        windowEnd = end,
        value = count,
        processingTime = Some(now)
      )
    )
  }
}


/**
 * Simple in-memory histogram for Flink metrics.
 */
private class CustomLatencyHistogram extends Histogram with Serializable {

  @transient private var values: mutable.ArrayBuffer[Long] = _

  private def ensureInit(): Unit = {
    if (values == null) {
      values = mutable.ArrayBuffer.empty[Long]
    }
  }

  override def update(value: Long): Unit = {
    ensureInit()
    values += value
  }

  override def getCount: Long = {
    if (values == null) 0L else values.size.toLong
  }

  override def getStatistics: HistogramStatistics = {
    ensureInit()
    new CustomHistogramStatistics(values.toArray)
  }
}

private class CustomHistogramStatistics(data: Array[Long]) extends HistogramStatistics {

  private val sorted: Array[Long] = data.sorted

  override def getMin: Long =
    if (sorted.isEmpty) 0L else sorted.head

  override def getMax: Long =
    if (sorted.isEmpty) 0L else sorted.last

  override def getMean: Double =
    if (sorted.isEmpty) 0.0 else sorted.map(_.toDouble).sum / sorted.length.toDouble

  override def getStdDev: Double = {
    if (sorted.length <= 1) 0.0
    else {
      val mean = getMean
      val variance =
        sorted.map(v => math.pow(v.toDouble - mean, 2)).sum / (sorted.length.toDouble - 1.0)
      math.sqrt(variance)
    }
  }

  override def getQuantile(quantile: Double): Double = {
    if (sorted.isEmpty) 0.0
    else {
      val q = math.min(1.0, math.max(0.0, quantile))
      val idx = math.round(q * (sorted.length - 1)).toInt
      sorted(idx).toDouble
    }
  }

  override def getValues: Array[Long] = data

  override def size: Int = data.length
}

object WindowAggregation {
  val countEvents = new CountEventsWindowFunction
}
