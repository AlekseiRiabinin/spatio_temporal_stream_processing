package cityrover

import com.datastax.oss.driver.api.core.CqlSession
import zio._
import zio.http._
import zio.kafka.consumer.{Consumer, ConsumerSettings, Offset}
import zio.metrics.Metric
import zio.metrics.connectors.prometheus.PrometheusPublisher
import zio.metrics.connectors.{MetricsConfig => ZioMetricsConfig, prometheus}
import zio.stream.ZStream

import cityrover.cassandra.{CassandraSchema, CassandraSession, CassandraSink}
import cityrover.kafka.{Consumer => KafkaConsumer}
import cityrover.metrics.LatencyMetrics
import cityrover.telemetry.EnrichedTelemetryEvent
import cityrover.util.{AppConfig, ConfigLoader}


object Main extends ZIOAppDefault:

  /** Metric registrations, built once at pipeline construction. */
  private final case class Metrics(
    kafkaToZio:  Metric.Histogram[Double],
    zioMs:       Metric.Histogram[Double],
    write:       Metric.Histogram[Double],
    latency:     Metric.Histogram[Double],
    consumed:    Metric.Counter[Long],
    written:     Metric.Counter[Long],
    writeErrors: Metric.Counter[Long]
  )

  private def buildMetrics(config: AppConfig): Metrics =
    Metrics(
      kafkaToZio  = LatencyMetrics.kafkaToZioMs(config.metrics, "writer"),
      zioMs       = LatencyMetrics.zioMs(config.metrics, "writer"),
      write       = LatencyMetrics.writeMs(config.metrics, "writer"),
      latency     = LatencyMetrics.latencyMs(config.metrics, "writer"),
      consumed    = LatencyMetrics.consumedRecords(config.metrics),
      written     = LatencyMetrics.writtenRecords(config.metrics),
      writeErrors = LatencyMetrics.writeErrors(config.metrics)
    )

  /** The pipeline: consume → write → commit, with metrics. */
  private def pipeline(
    config: AppConfig,
    sink: CassandraSink,
    metrics: Metrics
  ): ZStream[Consumer, Throwable, Unit] =
    KafkaConsumer
      .streamWithOffsets(config)
      .mapZIO { case (proto, offset, t1Ms, tSrcMs) =>
        val tWriteStartMs = java.lang.System.currentTimeMillis()

        // kafka_to_zio_ms: broker timestamp → parse done.
        val k2z: ZIO[Any, Nothing, Unit] =
          if t1Ms >= 0L then metrics.kafkaToZio.update((tSrcMs - t1Ms).toDouble)
          else ZIO.unit

        // zio_ms: parse done → sink start (queue time before the write).
        val z: ZIO[Any, Nothing, Unit] =
          metrics.zioMs.update((tWriteStartMs - tSrcMs).toDouble)

        // Success/failure side effects via tapError + tap, not tapBoth:
        // tapBoth requires the handler's error type to be a *supertype*
        // of the effect's E (Throwable), which a URIO (Nothing) is not.
        // tapError and tap both accept Nothing-error handlers.
        val writeEffect: ZIO[Any, Throwable, Unit] =
          sink.write(proto)
            .tapError(_ => metrics.writeErrors.update(1))
            .tap(_ => metrics.written.update(1))

        val t2Effect: ZIO[Any, Throwable, Unit] =
          ZIO.succeed(java.lang.System.currentTimeMillis()).flatMap { t2Ms =>
            val w = metrics.write.update((t2Ms - tWriteStartMs).toDouble)
            val l =
              if t1Ms >= 0L then metrics.latency.update((t2Ms - t1Ms).toDouble)
              else ZIO.unit
            w *> l
          }

        k2z *>
        z *>
        metrics.consumed.update(1) *>
        writeEffect *>
        t2Effect *>
        offset.commit
      }
      // No trailing .unit: mapZIO's body is ZIO[Any, Throwable, Unit],
      // so the resulting ZStream's element type is already Unit.

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
        s"Starting feature-writer-zio: " +
        s"enrichedTopic=${config.kafka.enrichedTopic}, " +
        s"keyspace=${config.cassandra.keyspace}, table=${config.cassandra.table}"
      )

      // 1. Bootstrap: create keyspace + table once. Separate short-lived session.
      _ <- ZIO.scoped {
        CassandraSession.bootstrap.build.flatMap { env =>
          val session = env.get[CqlSession]
          ZIO.attempt {
            CassandraSchema.initialize(
              session,
              config.cassandra.keyspace,
              config.cassandra.table
            )
          }
        }
      }

      // 2. Build metrics once.
      metrics = buildMetrics(config)

      // 3. HTTP server for /metrics, running concurrently with the pipeline.
      serverFiber <- Server
        .serve(metricsRoute)
        .provide(
          Server.default,
          prometheus.publisherLayer,
          prometheus.prometheusLayer,
          ZLayer.succeed(ZioMetricsConfig(5.seconds))
        )
        .fork

      // 4. Runtime session + sink layer + consumer layer, then run the pipeline.
      consumerSettings = KafkaConsumer.settings(config)
      _ <- ZIO.scoped {
        for
          session <- CassandraSession.runtime.build.map(_.get[CqlSession])
          sink    = new CassandraSink(session, config.cassandra)
          _       <- pipeline(config, sink, metrics)
                       .runDrain
                       .provideSomeLayer(
                         ZLayer.scoped(Consumer.make(consumerSettings))
                       )
        yield ()
      }

      _ <- serverFiber.interrupt
    yield ()).provide(ConfigLoader.live)
