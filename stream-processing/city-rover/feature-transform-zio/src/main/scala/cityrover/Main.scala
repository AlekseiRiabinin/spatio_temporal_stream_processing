package cityrover

import zio._
import zio.http._
import zio.kafka.consumer.Consumer
import zio.kafka.consumer.OffsetBatch
import zio.kafka.producer.Producer
import zio.metrics.connectors.prometheus.PrometheusPublisher
import zio.metrics.connectors.{MetricsConfig => ZioMetricsConfig, prometheus}
import zio.metrics.Metric.Histogram
import zio.stream.ZStream

import cityrover.kafka.{Consumer => KafkaConsumer, Producer => KafkaProducer, StampedEvent}
import cityrover.metrics.LatencyMetrics
import cityrover.model.{
  EnrichedEvent,
  EnrichedEvent5s,
  EnrichedEvent30s,
  EnrichedEvent1m,
  EnrichedEvent5m,
  FeatureEnvelope,
  TelemetryEvent
}
import cityrover.util.{AppConfig, ConfigLoader}
import cityrover.window.{EventTimeWindow, WindowState}


object Main extends ZIOAppDefault:

  /** Build the shared `FeatureEnvelope` for a fired window.
    *
    * `lastEvent` is the true last event in the window, extracted by
    * `EventTimeWindow.fireReadyWindows` from the accumulator's
    * `lastEvent` field. `windowEndTs` is the event-time boundary in ms.
    */
  private def envelopeFor(
    lastEvent: TelemetryEvent,
    windowEndTs: Long
  ): FeatureEnvelope =
    FeatureEnvelope(
      roverId = lastEvent.roverId,
      edgeId = lastEvent.edgeId,
      routeId = lastEvent.routeId,
      lat = lastEvent.lat,
      lon = lastEvent.lon,
      ts = lastEvent.ts,
      speed = lastEvent.speed,
      heading = lastEvent.heading,
      windowEndTs = windowEndTs,
      gridCellId = 0L,
      regionId = "",
      snappedEdgeId = lastEvent.edgeId,
      snappedLat = lastEvent.lat,
      snappedLon = lastEvent.lon,
      routeProgress = 0.0,
      routeDeviation = 0.0,
      expectedSpeed = 0.0,
      speedRatio = 0.0,
      drivingStyleScore = 0.0,
      anomalyScore = 0.0,
      featureVersion = "v1",
      featureTimestamp = windowEndTs,
      featureLatencyMs = 0L,
      isOutlier = false,
      isGpsJump = false,
      isSpeedAnomaly = false,
      isHeadingAnomaly = false,
      rawEventHash = lastEvent.hashCode().toHexString,
      processingNode = "zio-transform",
      processingTimeMs = 0L
    )

  /** Map an enriched event to its per-window subgroup label. */
  private def subgroupOf(ev: EnrichedEvent): String = ev match
    case _: EnrichedEvent5s  => "window_5s"
    case _: EnrichedEvent30s => "window_30s"
    case _: EnrichedEvent1m  => "window_1m"
    case _: EnrichedEvent5m  => "window_5m"

  /** Metric registrations, built once at pipeline construction.
    *
    * `LatencyMetrics`'s `def`s return a `Metric` value per call; calling
    * them once here (rather than per event) is important because
    * `Metric.histogram(...)` does a global registration on first use and
    * allocates a fresh key type each time.
    */
  private final case class Metrics(
    kafkaToFlink: Histogram[Double],
    queue: Histogram[Double],
    transform: Histogram[Double],
    emit: Histogram[Double],
    latency: Histogram[Double]
  )

  private def buildMetrics(config: AppConfig): Metrics =
    Metrics(
      kafkaToFlink = LatencyMetrics.kafkaToFlinkMs(config.metrics, "transform"),
      queue        = LatencyMetrics.queueMs(config.metrics, "transform"),
      transform    = LatencyMetrics.transformMs(config.metrics, "transform"),
      emit         = LatencyMetrics.emitMs(config.metrics, "transform"),
      latency      = LatencyMetrics.latencyMs(config.metrics, "transform")
    )

  /** The pipeline: consume → window → produce → commit, with metrics.
    *
    * `mapAccum` carries `(StampedEvent, Offset)` from the consumer and
    * emits `(offset, fired, stamped, tStart, tDone)` so `mapChunksZIO`
    * has everything it needs to produce, record `emit_ms`/`latency_ms`,
    * and commit.
    */
  private def pipeline(
    config: AppConfig,
    metrics: Metrics
  ): ZStream[Consumer & Producer, Throwable, Nothing] =
    KafkaConsumer
      .streamWithOffsets(config)
      .mapAccum(WindowState.empty) { (state, pair) =>
        val (stamped, offset) = pair

        // kafka_to_flink_ms: from broker timestamp to the moment we
        // started processing this record.
        val tTransformStartMs = java.lang.System.currentTimeMillis()

        val (newState, fired) =
          EventTimeWindow.step(
            state,
            stamped.event,
            config.window,
            tTransformStartMs,
            envelopeFor
          )

        val tTransformDoneMs = java.lang.System.currentTimeMillis()

        // Per-element metrics whose endpoints are now known. These are
        // recorded inside mapAccum because they need the single-event
        // timing context; the effectful `.update` calls are deferred to
        // mapChunksZIO to avoid per-element effect allocation when the
        // chunk is large.
        //
        // queue_ms = t_transform_start - t_src
        // transform_ms = t_transform_done - t_transform_start
        //
        // Note: kafka_to_flink_ms is captured later (in mapChunksZIO) so
        // it can be skipped if t1Ms < 0. Same for emit_ms / latency_ms.
        (
          newState,
          (offset, fired, stamped, tTransformStartMs, tTransformDoneMs)
        )
      }
      .mapChunksZIO { chunk =>
        val pairs = chunk.toSeq

        val allOffsets = OffsetBatch(pairs.map(_._1))
        val allFired   = pairs.flatMap(_._2)

        // Record per-element metrics (kafka_to_flink, queue, transform)
        // for every consumed record.
        val elementMetrics: ZIO[Any, Nothing, Unit] =
          ZIO.foreachDiscard(pairs) { case (_, _, stamped, tStart, tDone) =>
            val k2f =
              if stamped.t1Ms >= 0L then
                metrics.kafkaToFlink.update((stamped.tSrcMs - stamped.t1Ms).toDouble)
              else ZIO.unit
            val q  = metrics.queue.update((tStart - stamped.tSrcMs).toDouble)
            val t  = metrics.transform.update((tDone - tStart).toDouble)
            k2f *> q *> t
          }

        // Record emit_ms and latency_ms for each fired event, dispatched
        // by subgroup. These use the same Metrics values; the histogram
        // name is shared, but the subgroup label distinguishes windows.
        // If you want per-window histograms, call LatencyMetrics.* per
        // subgroup instead — see note below.
        val tSinkEnqueueMs = java.lang.System.currentTimeMillis()
        val firedMetrics: ZIO[Any, Nothing, Unit] =
          ZIO.foreachDiscard(allFired) { ev =>
            val _subgroup = subgroupOf(ev) // reserved for per-subgroup histograms
            metrics.emit.update((tSinkEnqueueMs - pairs.head._5).toDouble) *>
            metrics.latency.update((tSinkEnqueueMs - pairs.head._3.t1Ms).toDouble)
          }

        val produceEffect: ZIO[Producer, Throwable, Unit] =
          if allFired.isEmpty then ZIO.unit
          else KafkaProducer.produceChunk(config, Chunk.fromIterable(allFired)).unit

        elementMetrics *>
        produceEffect *>
        firedMetrics *>
        allOffsets.commit.as(Chunk.empty)
      }
      .drain

  /** HTTP endpoint that serves Prometheus metrics. */
  private val metricsRoute: Routes[PrometheusPublisher, Response] =
    Routes(
      Method.GET / "metrics" ->
        handler(
          ZIO.serviceWithZIO[PrometheusPublisher](_.get.map(Response.text))
        )
    )

  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    (for
      config <- ZIO.service[AppConfig]
      _ <- ZIO.logInfo(
        s"Starting feature-transform-zio: " +
        s"raw=${config.kafka.rawTopic}, enriched=${config.kafka.enrichedTopic}"
      )
      metrics = buildMetrics(config)
      consumerSettings = KafkaConsumer.settings(config)
      producerSettings = KafkaProducer.settings(config)

      // HTTP server for /metrics, running concurrently with the pipeline.
      serverFiber <- Server
        .serve(metricsRoute)
        .provide(
          Server.default,
          prometheus.publisherLayer,
          prometheus.prometheusLayer,
          ZLayer.succeed(ZioMetricsConfig(5.seconds))
        )
        .fork

      _ <- pipeline(config, metrics).runDrain
             .provideSomeLayer(
               ZLayer.scoped(Consumer.make(consumerSettings)) ++
               ZLayer.scoped(Producer.make(producerSettings))
             )

      _ <- serverFiber.interrupt
    yield ()).provide(ConfigLoader.live)
