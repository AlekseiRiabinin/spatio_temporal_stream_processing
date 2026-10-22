package cityrover.cassandra

import cityrover.telemetry.{
  EnrichedTelemetryEvent,
  Window5sFeatures,
  Window30sFeatures,
  Window1mFeatures,
  Window5mFeatures
}
import cityrover.metrics.{LatencyMetrics, StampedRecord}
import cityrover.util.MetricsConfig

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.typesafe.config.Config

import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

import java.util.ArrayList
import java.util.concurrent.{CompletionStage, Semaphore}


case class CassandraConnectorConfig(config: Config)


final class CassandraSink(
  connectorConfig: CassandraConnectorConfig,
  metricsCfg: MetricsConfig
) extends Sink[StampedRecord[EnrichedTelemetryEvent]]:

  override def createWriter(
    context: WriterInitContext
  ): SinkWriter[StampedRecord[EnrichedTelemetryEvent]] =
    new CassandraSinkWriter(connectorConfig, metricsCfg, context)

end CassandraSink


final class CassandraSinkWriter(
  connectorConfig: CassandraConnectorConfig,
  metricsCfg: MetricsConfig,
  initContext: WriterInitContext
) extends SinkWriter[StampedRecord[EnrichedTelemetryEvent]]:

  private val log = LoggerFactory.getLogger(getClass)

  // ---------------------------------------------------------------------------
  // Latency registry, built per-subtask in the constructor.
  // ---------------------------------------------------------------------------

  private val lat: LatencyMetrics =
    LatencyMetrics.register(initContext.metricGroup(), metricsCfg, "writer")

  // ---------------------------------------------------------------------------
  // Bounded in-flight writes: when `maxInFlight` async writes are pending,
  // `write` blocks on `acquire()`. This is how Flink backpressures the
  // Kafka consumer when Cassandra is slow, instead of growing
  // `pendingWrites` without bound.
  // ---------------------------------------------------------------------------

  private val maxInFlight = 1024
  private val permits     = new Semaphore(maxInFlight)

  private final case class PendingWrite(
    future: CompletionStage[AsyncResultSet],
    t1Ms: Double,
    tSrcMs: Double,
    tWriteStartMs: Double
  )

  private val pendingWrites = new ArrayList[PendingWrite]()

  // ---------------------------------------------------------------------------
  // Initialize Cassandra connector with schema (bootstrap session).
  // ---------------------------------------------------------------------------

  private val connector: CassandraConnector =
    val config   = connectorConfig.config
    val keyspace = config.getString("cityrover.cassandra.keyspace")
    val table    = config.getString("cityrover.cassandra.table")

    val bootstrapSession = CassandraConnector.createBootstrapSession(config)
    try CassandraSchema.initialize(bootstrapSession, keyspace, table)
    finally bootstrapSession.close()

    CassandraConnector.createRuntimeConnector(config)

  // ---------------------------------------------------------------------------
  // Write one partial record. Dispatch on the `oneof` variant, bind only
  // that window's columns, and use event-time (windowEndTs * 1000) as the
  // Cassandra write timestamp so replays and reordering are idempotent.
  // ---------------------------------------------------------------------------

  override def write(
    stamped: StampedRecord[EnrichedTelemetryEvent],
    context: SinkWriter.Context
  ): Unit =
    try
      val event    = stamped.payload
      val tsMicros = event.windowEndTs * 1000L   // ms -> µs

      permits.acquire()   // backpressure: block if maxInFlight reached
      val tWriteStartMs = System.currentTimeMillis().toDouble

      // Metrics whose endpoints are already known.
      val t1Ms: Double =
        if stamped.t1Ms >= 0L then stamped.t1Ms.toDouble else -1.0
      val tSrcMs = stamped.tSrcMs
      if t1Ms >= 0.0 then lat.recordKafkaToFlink(t1Ms, tSrcMs)
      lat.recordFlink(tSrcMs, tWriteStartMs)

      // Bind per window. Exactly one variant is set (validated by
      // EnrichedEventSchema upstream); anything else is a bug.
      val bound =
        event.features match
          case EnrichedTelemetryEvent.Features.W5S(f)  =>
            connector.bind5s(event, f, tsMicros)
          case EnrichedTelemetryEvent.Features.W30S(f) =>
            connector.bind30s(event, f, tsMicros)
          case EnrichedTelemetryEvent.Features.W1M(f)  =>
            connector.bind1m(event, f, tsMicros)
          case EnrichedTelemetryEvent.Features.W5M(f)  =>
            connector.bind5m(event, f, tsMicros)
          case other =>
            throw new IllegalStateException(
              s"Unknown/empty features variant for roverId='${event.roverId}': $other"
            )

      val future = connector.session.executeAsync(bound)

      // Completion callback: record write_ms / latency_ms, release the
      // backpressure permit, and log failures. Runs on the driver thread,
      // so it must be fast and must not throw.
      val _ = future.whenComplete { (_, error) =>
        permits.release()
        val t2Ms = System.currentTimeMillis().toDouble
        if error == null then
          lat.recordWrite(tWriteStartMs, t2Ms)
          if t1Ms >= 0.0 then lat.recordEndToEnd(t1Ms, t2Ms)
        else
          log.debug(s"Async Cassandra write failed: ${error.getMessage}")
      }

      pendingWrites.synchronized:
        pendingWrites.add(PendingWrite(future, t1Ms, tSrcMs, tWriteStartMs))
      cleanupCompletedFutures()

    catch
      case ex: Exception =>
        // If we threw before submitting, release the permit we acquired.
        // (If the throw came from `executeAsync` itself, the future is null
        // and no `whenComplete` will release it — so we release here.)
        permits.release()
        log.error(
          s"Failed to write enriched telemetry event to Cassandra: " +
          s"roverId=${stamped.payload.roverId}, windowType=${stamped.payload.windowType}",
          ex
        )

  // ---------------------------------------------------------------------------
  // Flush / close
  // ---------------------------------------------------------------------------

  override def flush(endOfInput: Boolean): Unit =
    log.info(s"Flushing Cassandra writes, endOfInput=$endOfInput")
    waitForPendingWrites()

  override def close(): Unit =
    log.info("Closing CassandraSinkWriter...")
    try waitForPendingWrites()
    catch case ex: Exception => log.warn("Error while waiting for pending writes", ex)
    finally
      connector.close()
      log.info("CassandraSinkWriter closed.")

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def waitForPendingWrites(): Unit =
    pendingWrites.synchronized:
      if !pendingWrites.isEmpty then
        log.info(s"Waiting for ${pendingWrites.size} pending async writes...")
        try
          pendingWrites.forEach(pw => pw.future.toCompletableFuture.join())
        catch case ex: Exception => log.error("Error while waiting for pending writes", ex)
        finally pendingWrites.clear()
      else
        log.debug("No pending writes to wait for")

  private def cleanupCompletedFutures(): Unit =
    pendingWrites.synchronized:
      val it = pendingWrites.iterator()
      while it.hasNext do
        val pw = it.next()
        if pw.future.toCompletableFuture.isDone then it.remove()

end CassandraSinkWriter
