package cityrover.metrics

import com.codahale.metrics.{
  Histogram => DropwizardHistogram,
  SlidingWindowReservoir,
  ExponentiallyDecayingReservoir
}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Histogram, MetricGroup}

import java.util.concurrent.atomic.AtomicLong

import cityrover.util.{MetricsConfig, RegisterToggles, ReservoirConfig}


/**
 * Registry for latency histograms used by the Scala Flink jobs.
 *
 * Mirrors the metric decomposition implemented in the PyFlink probes
 * (kafka_latency_probe.py, cassandra_latency_probe.py):
 *
 *   kafka_to_flink_ms = t_src_ms             - t1_ms
 *   queue_ms          = t_transform_start_ms - t_src_ms
 *   transform_ms      = t_transform_done_ms  - t_transform_start_ms
 *   emit_ms           = t_sink_enqueue_ms    - t_transform_done_ms
 *   latency_ms        = t_sink_enqueue_ms    - t1_ms
 *
 *   latency_ms == kafka_to_flink_ms + queue_ms + transform_ms + emit_ms
 *
 * Each metric is a Flink Histogram backed by a Dropwizard reservoir. Flink's
 * Prometheus reporter exposes histograms as:
 *   <name>_count
 *   <name>_sum
 *   <name>{quantile="0.5"|"0.95"|"0.99"}
 *
 * Usage:
 *   // In an operator's open():
 *   val lat = LatencyMetrics.register(runtimeContext, cfg, "transform")
 *
 *   // On every record:
 *   lat.recordKafkaToFlink(t1Ms, tSrcMs)
 *   lat.recordQueue(tSrcMs, tTransformStartMs)
 *   lat.recordTransform(tTransformStartMs, tTransformDoneMs)
 *   lat.recordEmit(tTransformDoneMs, tSinkEnqueueMs)
 *   lat.recordEndToEnd(t1Ms, tSinkEnqueueMs)
 *
 * Every histogram is optional — the `register` toggles in MetricsConfig
 * determine which ones are actually created. Unregistered stages are
 * silently ignored (methods become no-ops).
 */
final class LatencyMetrics private (
  private val kafkaToFlinkHist: Option[Histogram],
  private val queueHist:        Option[Histogram],
  private val transformHist:    Option[Histogram],
  private val emitHist:         Option[Histogram],
  private val endToEndHist:     Option[Histogram],
  private val sampleRate:       Double
):

  // ---------------------------------------------------------------
  // Recording API — one method per latency stage.
  // Each takes the two wall-clock endpoints (in ms) of the stage.
  // ---------------------------------------------------------------

  def recordKafkaToFlink(t1Ms: Double, tSrcMs: Double): Unit =
    record(kafkaToFlinkHist, tSrcMs - t1Ms)

  def recordQueue(tSrcMs: Double, tTransformStartMs: Double): Unit =
    record(queueHist, tTransformStartMs - tSrcMs)

  def recordTransform(tTransformStartMs: Double, tTransformDoneMs: Double): Unit =
    record(transformHist, tTransformDoneMs - tTransformStartMs)

  def recordEmit(tTransformDoneMs: Double, tSinkEnqueueMs: Double): Unit =
    record(emitHist, tSinkEnqueueMs - tTransformDoneMs)

  def recordEndToEnd(t1Ms: Double, tSinkEnqueueMs: Double): Unit =
    record(endToEndHist, tSinkEnqueueMs - t1Ms)

  // ---------------------------------------------------------------
  // Internal sampling + recording.
  //
  // `sampleRate` is applied per-record. With sampleRate = 1.0 every
  // call records; with 0.1, roughly one in ten calls records. This
  // keeps overhead tunable for very high-throughput jobs.
  // ---------------------------------------------------------------

  private def record(hist: Option[Histogram], latencyMs: Double): Unit =
    hist.foreach { h =>
      if sampleRate >= 1.0 || math.random() < sampleRate then
        // Dropwizard histograms accept Long values only.
        // Rounding to the nearest ms is fine for this scale; if sub-ms
        // precision is needed, store microseconds and divide in Grafana.
        h.update(math.round(latencyMs))
    }

end LatencyMetrics


object LatencyMetrics:

  /**
   * Register all configured latency histograms under a subgroup of the
   * operator's MetricGroup.
   *
   * @param ctx        operator's RuntimeContext (provides the MetricGroup)
   * @param cfg        parsed metrics configuration from ConfigLoader
   * @param subgroup   per-operator subgroup name, e.g. "transform" or "writer"
   *                   (becomes part of the Prometheus metric name)
   */
  def register(
    ctx: RuntimeContext,
    cfg: MetricsConfig,
    subgroup: String
  ): LatencyMetrics =

    if !cfg.prometheusEnabled then
      // Metrics disabled — return a no-op registry.
      // Every recording method becomes a no-op.
      return new LatencyMetrics(None, None, None, None, None, 0.0)

    val group: MetricGroup =
      buildGroup(ctx.getMetricGroup, cfg.metricGroup, subgroup, cfg.tags)

    val toggles = cfg.register
    val res     = cfg.reservoir

    new LatencyMetrics(
      kafkaToFlinkHist = Option.when(toggles.kafkaToFlink)(hist(group, "kafka_to_flink_ms", res)),
      queueHist        = Option.when(toggles.queue)(hist(group, "queue_ms", res)),
      transformHist    = Option.when(toggles.transform)(hist(group, "transform_ms", res)),
      emitHist         = Option.when(toggles.emit)(hist(group, "emit_ms", res)),
      endToEndHist     = Option.when(toggles.endToEnd)(hist(group, "latency_ms", res)),
      sampleRate       = cfg.sampleRate
    )

  // ---------------------------------------------------------------
  // Group construction.
  //
  // Flink MetricGroups are hierarchical: each `addGroup` adds a segment
  // to the metric name. We start at the operator's own group (which
  // Flink populates with job/task/operator info) and add:
  //
  //   <cfg.metricGroup>.<subgroup>[.<tagKey>=<tagValue>...]
  //
  // Tags are added as nested groups. Prometheus renders these as labels.
  // ---------------------------------------------------------------

  private def buildGroup(
    base: MetricGroup,
    metricGroup: String,
    subgroup: String,
    tags: Map[String, String]
  ): MetricGroup =
    val g = base.addGroup(metricGroup).addGroup(subgroup)
    tags.foldLeft(g) { case (acc, (k, v)) => acc.addGroup(k, v) }

  // ---------------------------------------------------------------
  // Histogram construction.
  //
  // Two reservoir types are supported:
  //
  //   sliding-window          — keeps the last N samples, exact percentiles
  //                             over that window. Memory: O(N) per subtask.
  //   exponentially-decaying  — continuously weighted, biased to recent.
  //                             Cheaper (constant memory), less accurate.
  //
  // For a latency probe, sliding-window is recommended (default).
  // ---------------------------------------------------------------

  private def hist(
    group: MetricGroup,
    name: String,
    res: ReservoirConfig
  ): Histogram =
    val dw: DropwizardHistogram = res.kind match
      case "sliding-window" =>
        new DropwizardHistogram(new SlidingWindowReservoir(res.windowSize))
      case "exponentially-decaying" =>
        new DropwizardHistogram(new ExponentiallyDecayingReservoir())
      case other =>
        throw new IllegalArgumentException(
          s"Unknown reservoir type '$other' (expected 'sliding-window' or 'exponentially-decaying')"
        )

    group.histogram(name, new DropwizardHistogramWrapper(dw))
