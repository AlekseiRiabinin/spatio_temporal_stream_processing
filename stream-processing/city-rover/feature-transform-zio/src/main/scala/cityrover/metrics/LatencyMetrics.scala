package cityrover.metrics

import zio._
import zio.metrics.{Metric, MetricKeyType, MetricLabel}

import cityrover.util.MetricsConfig


/** Latency histograms and counters for feature-transform-zio.
  *
  * Mirrors the metric surface of the Flink `LatencyMetrics`:
  *   - kafka_to_flink_ms : t_src_ms - t1_ms
  *   - queue_ms          : t_transform_start_ms - t_src_ms
  *   - transform_ms      : t_transform_done_ms - t_transform_start_ms
  *   - emit_ms           : t_sink_enqueue_ms - t_transform_done_ms
  *   - latency_ms        : t_sink_enqueue_ms - t1_ms
  *
  * Plus counters for observability of the recording paths themselves.
  *
  * ZIO metrics are global and name-keyed; registration happens implicitly
  * on first use. The Prometheus connector in `Main` reads them at scrape
  * time. There is no per-subtask registry like Flink's RuntimeContext.
  *
  * `namespace` and `subgroup` are attached as MetricLabels so Prometheus
  * can distinguish this job from the Flink one and filter by window stage.
  */
object LatencyMetrics:

  /** Build the histogram boundaries from the config.
    *
    * `MetricKeyType.Histogram.Boundaries` lives directly under `zio.metrics`,
    * not nested inside `Metric`[citation:2][citation:9].
    *
    * `fromChunk` appends `Double.MaxValue` automatically, so the resulting
    * boundaries always satisfy the invariant that the last bucket captures
    * everything[citation:1].
    */
  def boundaries(config: MetricsConfig): MetricKeyType.Histogram.Boundaries =
    val buckets = config.buckets
    if buckets.isEmpty then
      MetricKeyType.Histogram.Boundaries.exponential(0.5, 2.0, 12)
    else
      MetricKeyType.Histogram.Boundaries.fromChunk(buckets)

  /** Attach namespace + subgroup labels to a histogram. */
  private def tagHistogram(
    m: Metric.Histogram[Double],
    config: MetricsConfig,
    subgroup: String
  ): Metric.Histogram[Double] =
    m.tagged(
      MetricLabel("namespace", config.namespace),
      MetricLabel("subgroup", subgroup)
    )

  // ---------------------------------------------------------------------------
  // Histograms — one per latency stage.
  //
  // `Metric.Histogram[Double]` is the type alias for
  // `Metric[MetricKeyType.Histogram, Double, MetricState.Histogram]`[citation:5][citation:8].
  // Defs, not vals: boundaries depend on config, so callers build them
  // once in Main and reuse the resulting values.
  // ---------------------------------------------------------------------------

  def kafkaToFlinkMs(config: MetricsConfig, subgroup: String): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("kafka_to_flink_ms", boundaries(config)),
      config, subgroup
    )

  def queueMs(config: MetricsConfig, subgroup: String): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("queue_ms", boundaries(config)),
      config, subgroup
    )

  def transformMs(config: MetricsConfig, subgroup: String): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("transform_ms", boundaries(config)),
      config, subgroup
    )

  def emitMs(config: MetricsConfig, subgroup: String): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("emit_ms", boundaries(config)),
      config, subgroup
    )

  def latencyMs(config: MetricsConfig, subgroup: String): Metric.Histogram[Double] =
    tagHistogram(
      Metric.histogram("latency_ms", boundaries(config)),
      config, subgroup
    )

  // ---------------------------------------------------------------------------
  // Counters — observability of the recording paths themselves.
  //
  // `Metric.Counter[Long]` is the type alias for
  // `Metric[MetricKeyType.Counter, Long, MetricState.Counter]`[citation:5].
  // ---------------------------------------------------------------------------

  def taggedRecords(config: MetricsConfig): Metric.Counter[Long] =
    Metric.counter("tagged_records_total")
      .tagged(MetricLabel("namespace", config.namespace))

  def kafkaToFlinkRecorded(config: MetricsConfig, subgroup: String): Metric.Counter[Long] =
    Metric.counter("kafka_to_flink_recorded_here_total")
      .tagged(MetricLabel("namespace", config.namespace), MetricLabel("subgroup", subgroup))

  def transformErrors(config: MetricsConfig, subgroup: String): Metric.Counter[Long] =
    Metric.counter("transform_errors_total")
      .tagged(MetricLabel("namespace", config.namespace), MetricLabel("subgroup", subgroup))
