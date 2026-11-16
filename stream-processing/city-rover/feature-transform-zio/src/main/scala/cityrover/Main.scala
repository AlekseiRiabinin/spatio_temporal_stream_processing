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

  /** Metric registrations, built once at pipeline construction. */
  private final case class Metrics(
    kafkaToZio: Histogram[Double],
    queue: Histogram[Double],
    transform: Histogram[Double],
    emit: Histogram[Double],
    latency: Histogram[Double]
  )

  private def buildMetrics(config: AppConfig): Metrics =
    Metrics(
      kafkaToZio = LatencyMetrics.kafkaToZioMs(config.metrics, "transform"),
      queue      = LatencyMetrics.queueMs(config.metrics, "transform"),
      transform  = LatencyMetrics.transformMs(config.metrics, "transform"),
      emit       = LatencyMetrics.emitMs(config.metrics, "transform"),
      latency    = LatencyMetrics.latencyMs(config.metrics, "transform")
    )

  /** The pipeline: consume → window → produce → commit, with metrics.
    *
    * `mapAccum` carries `(StampedEvent, Offset)` from the consumer and
    * emits per-input:
    *   - the raw offset (for commit)
    *   - the StampedEvent + transform start/done (for queue/transform
    *     metrics — one observation per consumed event)
    *   - a chunk of `FiredWithTiming` (one per fired enriched event,
    *     each carrying its own timing context for emit/latency metrics)
    */
  private final case class FiredWithTiming(
    event: EnrichedEvent,
    t1Ms: Long,
    tSrcMs: Long,
    tTransformStartMs: Long,
    tTransformDoneMs: Long
  )

  private def pipeline(
    config: AppConfig,
    metrics: Metrics
  ): ZStream[Consumer & Producer, Throwable, Nothing] =
    KafkaConsumer
      .streamWithOffsets(config)
      .mapAccum(WindowState.empty) { (state, pair) =>
        val (stamped, offset) = pair

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

        // Attach this pair's timing to every fired event, so emit_ms
        // and latency_ms use the correct pair's t1/tSrc/tDone, not the
        // first pair in the chunk.
        val firedWithTiming: Chunk[FiredWithTiming] =
          fired.map(ev =>
            FiredWithTiming(
              event = ev,
              t1Ms = stamped.t1Ms,
              tSrcMs = stamped.tSrcMs,
              tTransformStartMs = tTransformStartMs,
              tTransformDoneMs = tTransformDoneMs
            )
          )

        (
          newState,
          (offset, stamped, tTransformStartMs, tTransformDoneMs, firedWithTiming)
        )
      }
      .mapChunksZIO { chunk =>
        val pairs = chunk.toSeq

        val allOffsets = OffsetBatch(pairs.map(_._1))
        val allFired   = pairs.flatMap(_._5)

        // queue_ms / transform_ms / kafka_to_zio_ms: one observation
        // per consumed event, using the pair's own timing.
        val elementMetrics: ZIO[Any, Nothing, Unit] =
          ZIO.foreachDiscard(pairs) { case (_, stamped, tStart, tDone, _) =>
            val k2z =
              if stamped.t1Ms >= 0L then
                metrics.kafkaToZio.update((stamped.tSrcMs - stamped.t1Ms).toDouble)
              else ZIO.unit
            val q = metrics.queue.update((tStart - stamped.tSrcMs).toDouble)
            val t = metrics.transform.update((tDone - tStart).toDouble)
            k2z *> q *> t
          }

        // emit_ms / latency_ms: one observation per fired event, using
        // THAT event's timing context (not pairs.head).
        val tSinkEnqueueMs = java.lang.System.currentTimeMillis()
        val firedMetrics: ZIO[Any, Nothing, Unit] =
          ZIO.foreachDiscard(allFired) { f =>
            val _subgroup = subgroupOf(f.event) // reserved for per-subgroup histograms
            val e = metrics.emit.update((tSinkEnqueueMs - f.tTransformDoneMs).toDouble)
            val l =
              if f.t1Ms >= 0L then
                metrics.latency.update((tSinkEnqueueMs - f.t1Ms).toDouble)
              else ZIO.unit
            e *> l
          }

        val firedEvents: Chunk[EnrichedEvent] = Chunk.fromIterable(allFired.map(_.event))

        val produceEffect: ZIO[Producer, Throwable, Unit] =
          if firedEvents.isEmpty then ZIO.unit
          else KafkaProducer.produceChunk(config, firedEvents).unit

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
        s"raw=${config.kafka.rawTopic}, enriched=${config.kafka.enrichedTopic}, " +
        s"metricsPort=${config.metrics.httpPort}"
      )
      metrics = buildMetrics(config)
      consumerSettings = KafkaConsumer.settings(config)
      producerSettings = KafkaProducer.settings(config)

      // HTTP server for /metrics on the configured port (9084).
      // Server.default binds to 8080; we need ServerConfig.default.port(...)
      // to bind to config.metrics.httpPort, matching the compose port
      // mapping and the Prometheus scrape target.
      serverConfig = ZLayer.succeed(
        Server.Config.default.port(config.metrics.httpPort)
      )

      serverFiber <- Server
        .serve(metricsRoute)
        .provide(
          serverConfig,
          Server.live,
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
