package cityrover.metrics

import com.codahale.metrics.{
  Histogram => DropwizardHistogram,
  SlidingWindowReservoir,
  ExponentiallyDecayingReservoir
}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Histogram, MetricGroup}

import cityrover.util.{MetricsConfig, ReservoirConfig}


/**
 * Registry for latency histograms used by the feature-writer-job.
 *
 * Mirrors the metric decomposition implemented in the PyFlink probe
 * cassandra_latency_probe.py:
 *
 *   t1_ms             = Kafka broker timestamp (ctx.timestamp())
 *   t_src_ms          = wall clock when record leaves source
 *   t_write_start_ms  = wall clock immediately before session.execute()
 *   t2_ms             = wall clock immediately after session.execute() returns
 *
 *   kafka_to_flink_ms = t_src_ms         - t1_ms
 *   flink_ms          = t_write_start_ms - t_src_ms
 *   write_ms          = t2_ms            - t_write_start_ms
 *   latency_ms        = t2_ms            - t1_ms
 *
 *   latency_ms == kafka_to_flink_ms + flink_ms + write_ms
 *
 * Each metric is a Flink Histogram backed by a Dropwizard reservoir.
 * Flink's Prometheus reporter exposes histograms as:
 *   <name>_count
 *   <name>_sum
 *   <name>{quantile="0.5"|"0.95"|"0.99"}
 *
 * Usage:
 *   // In a RichFunction operator (window function, rich map, …):
 *   val lat = LatencyMetrics.register(getRuntimeContext, cfg, "writer")
 *
 *   // In a SinkWriter (where there is no RuntimeContext, only a MetricGroup):
 *   val lat = LatencyMetrics.register(initContext.metricGroup(), cfg, "writer")
 *
 *   // On every record:
 *   lat.recordKafkaToFlink(t1Ms, tSrcMs)
 *   lat.recordFlink(tSrcMs, tWriteStartMs)
 *   lat.recordWrite(tWriteStartMs, t2Ms)
 *   lat.recordEndToEnd(t1Ms, t2Ms)
 *
 * Every histogram is optional — the `register` toggles in MetricsConfig
 * determine which ones are actually created. Unregistered stages are
 * silently ignored (methods become no-ops).
 */
final class LatencyMetrics private (
  private val kafkaToFlinkHist: Option[Histogram],
  private val flinkHist:        Option[Histogram],
  private val writeHist:        Option[Histogram],
  private val endToEndHist:     Option[Histogram],
  private val sampleRate:       Double
):

  // ---------------------------------------------------------------
  // Recording API — one method per latency stage.
  // Each takes the two wall-clock endpoints (in ms) of the stage.
  // ---------------------------------------------------------------

  def recordKafkaToFlink(t1Ms: Double, tSrcMs: Double): Unit =
    record(kafkaToFlinkHist, tSrcMs - t1Ms)

  /**
   * Time from source emission to the start of the Cassandra write.
   * This is what the PyFlink probe calls `flink_ms`.
   */
  def recordFlink(tSrcMs: Double, tWriteStartMs: Double): Unit =
    record(flinkHist, tWriteStartMs - tSrcMs)

  /**
   * Time spent inside session.execute() / waiting for the async write.
   * This is what the PyFlink probe calls `write_ms`.
   *
   * When the sink uses executeAsync + whenComplete, this measures the
   * true write completion time (from the driver's I/O thread). When the
   * sink uses execute, it measures the synchronous round trip.
   */
  def recordWrite(tWriteStartMs: Double, t2Ms: Double): Unit =
    record(writeHist, t2Ms - tWriteStartMs)

  /**
   * Full pipeline: from Kafka broker timestamp to after the Cassandra
   * write returns.
   */
  def recordEndToEnd(t1Ms: Double, t2Ms: Double): Unit =
    record(endToEndHist, t2Ms - t1Ms)

  // ---------------------------------------------------------------
  // Internal sampling + recording.
  //
  // `sampleRate` is applied per-record. With sampleRate = 1.0 every
  // call records; with 0.1, roughly one in ten calls records.
  // ---------------------------------------------------------------

  private def record(hist: Option[Histogram], latencyMs: Double): Unit =
    hist.foreach { h =>
      if sampleRate >= 1.0 || math.random() < sampleRate then
        // Dropwizard histograms accept Long values only.
        // Rounding to the nearest ms is fine at this scale.
        h.update(math.round(latencyMs))
    }

end LatencyMetrics


object LatencyMetrics:

  // ===============================================================
  // Registration overloads
  // ===============================================================

  /**
   * Register latency histograms given a RuntimeContext.
   *
   * Used by RichFunction operators — window functions, RichMapFunction,
   * RichFlatMapFunction, etc. — where `getRuntimeContext` is available
   * inside `open()`.
   *
   * Delegates to the MetricGroup overload below.
   */
  def register(
    ctx: RuntimeContext,
    cfg: MetricsConfig,
    subgroup: String
  ): LatencyMetrics =
    register(ctx.getMetricGroup, cfg, subgroup)

  /**
   * Register latency histograms given a MetricGroup directly.
   *
   * Used by SinkWriter implementations, where the metric group is
   * obtained from the WriterInitContext (`initContext.metricGroup()`)
   * rather than from a RuntimeContext.
   *
   * Metric name mapping (writer job):
   *   register.kafkaToFlink -> "kafka_to_flink_ms"
   *   register.queue        -> "flink_ms"
   *   register.transform    -> "write_ms"
   *   register.endToEnd     -> "latency_ms"
   *
   * The `emit` toggle is ignored in the writer job (no Kafka sink).
   *
   * @param base       Flink MetricGroup to attach histograms to
   * @param cfg        parsed metrics configuration from ConfigLoader
   * @param subgroup   per-operator subgroup name, e.g. "writer"
   */
  def register(
    base: MetricGroup,
    cfg: MetricsConfig,
    subgroup: String
  ): LatencyMetrics =

    if !cfg.prometheusEnabled then
      // Metrics disabled — return a no-op registry.
      // Every recording method becomes a no-op.
      return new LatencyMetrics(None, None, None, None, 0.0)

    val group: MetricGroup =
      buildGroup(base, cfg.metricGroup, subgroup, cfg.tags)

    val toggles = cfg.register
    val res     = cfg.reservoir

    new LatencyMetrics(
      kafkaToFlinkHist = Option.when(toggles.kafkaToFlink)(hist(group, "kafka_to_flink_ms", res)),
      flinkHist        = Option.when(toggles.queue)(hist(group, "flink_ms", res)),
      writeHist        = Option.when(toggles.transform)(hist(group, "write_ms", res)),
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
