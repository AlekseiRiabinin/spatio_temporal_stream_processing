package cityrover.cassandra

import cityrover.telemetry.EnrichedTelemetryEvent
import cityrover.metrics.{LatencyMetrics, StampedRecord}
import cityrover.util.MetricsConfig

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.typesafe.config.Config

import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

import java.util.ArrayList
import java.util.concurrent.{CompletionStage, CompletableFuture}


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
  //
  // In the SinkWriter lifecycle there is no `open()` method — the writer is
  // constructed by `createWriter`. We build the LatencyMetrics registry here,
  // using the metric group exposed by `initContext.metricGroup()`.
  // ---------------------------------------------------------------------------

  private val lat: LatencyMetrics =
    LatencyMetrics.register(
      initContext.metricGroup(),
      metricsCfg,
      "writer"
    )

  // ---------------------------------------------------------------------------
  // Pending futures, each paired with the timing context needed to record
  // write_ms and latency_ms once the write completes.
  // ---------------------------------------------------------------------------

  private final case class PendingWrite(
    future: CompletionStage[AsyncResultSet],
    t1Ms: Double,
    tSrcMs: Double,
    tWriteStartMs: Double
  )

  private val pendingWrites =
    new ArrayList[PendingWrite]()

  // ---------------------------------------------------------------------------
  // Initialize Cassandra connector with schema (bootstrap session)
  // ---------------------------------------------------------------------------

  private val connector: CassandraConnector =
    val config   = connectorConfig.config
    val keyspace = config.getString("cityrover.cassandra.keyspace")
    val table    = config.getString("cityrover.cassandra.table")

    val bootstrapSession =
      CassandraConnector.createBootstrapSession(config)

    try
      CassandraSchema.initialize(bootstrapSession, keyspace, table)
    finally
      bootstrapSession.close()

    CassandraConnector.createRuntimeConnector(config)

  // ---------------------------------------------------------------------------
  // Write enriched telemetry events asynchronously, with timing.
  //
  // The timing flow:
  //
  //   t1Ms         — Kafka broker timestamp (from StampedRecord.t1Ms if
  //                  populated, otherwise -1)
  //   tSrcMs       — wall clock at the source boundary (from StampedRecord)
  //   tWriteStart  — captured immediately before executeAsync()
  //   t2Ms         — captured inside whenComplete(), on the driver's thread
  //
  // kafka_to_flink_ms and flink_ms are recorded synchronously here because
  // their endpoints (t1, tSrc, tWriteStart) are all known.
  //
  // write_ms and latency_ms require t2, which is only known after the
  // future completes. They are recorded in the whenComplete callback.
  // ---------------------------------------------------------------------------

  override def write(
    stamped: StampedRecord[EnrichedTelemetryEvent],
    context: SinkWriter.Context
  ): Unit =
    try
      val event = stamped.payload

      // If t1 was not populated at the source boundary, try to recover it
      // from the SinkWriter context. This depends on the KafkaSource
      // assigning timestamps; if not, t1 stays -1 and the kafka_to_flink_ms
      // histogram is skipped for that record.
      val t1Ms: Double =
        if stamped.t1Ms >= 0L then stamped.t1Ms.toDouble
        else -1.0

      val tSrcMs = stamped.tSrcMs

      // Record the two stages whose endpoints are already known.
      if t1Ms >= 0.0 then
        lat.recordKafkaToFlink(t1Ms, tSrcMs)

      // Bound the pre-write phase (t_src -> t_write_start).
      val tWriteStartMs = System.currentTimeMillis().toDouble
      lat.recordFlink(tSrcMs, tWriteStartMs)

      // Submit the async write.
      val stmt   = connector.bind(event)
      val future = connector.session.executeAsync(stmt)

      // Attach a completion callback that records write_ms and latency_ms
      // once the future resolves. The callback runs on the driver's I/O
      // thread — it must be fast and must not throw.
      val _ = future.whenComplete { (_, error) =>
        val t2Ms = System.currentTimeMillis().toDouble
        if error == null then
          lat.recordWrite(tWriteStartMs, t2Ms)
          if t1Ms >= 0.0 then
            lat.recordEndToEnd(t1Ms, t2Ms)
        else
          // Cassandra write failed — do not record a latency sample for
          // the failed attempt. The error is logged by the writer.
          log.debug(s"Async Cassandra write failed: ${error.getMessage}")
      }

      // Track the future for flush()/close() to wait on.
      pendingWrites.synchronized:
        pendingWrites.add(PendingWrite(future, t1Ms, tSrcMs, tWriteStartMs))

      cleanupCompletedFutures()

    catch
      case ex: Exception =>
        log.error(
          s"Failed to write enriched telemetry event to Cassandra: ${stamped.payload}",
          ex
        )

  // ---------------------------------------------------------------------------
  // Flush pending async writes
  // ---------------------------------------------------------------------------

  override def flush(endOfInput: Boolean): Unit =
    log.info(s"Flushing Cassandra writes, endOfInput=$endOfInput")
    waitForPendingWrites()

  // ---------------------------------------------------------------------------
  // Close sink writer
  // ---------------------------------------------------------------------------

  override def close(): Unit =
    log.info("Closing CassandraSinkWriter...")

    try
      waitForPendingWrites()
    catch
      case ex: Exception =>
        log.warn("Error while waiting for pending writes", ex)
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
          pendingWrites.forEach { pw =>
            pw.future.toCompletableFuture.join()
          }
        catch
          case ex: Exception =>
            log.error("Error while waiting for pending writes", ex)
        finally
          pendingWrites.clear()
      else
        log.debug("No pending writes to wait for")

  private def cleanupCompletedFutures(): Unit =
    pendingWrites.synchronized:
      val it = pendingWrites.iterator()
      while it.hasNext do
        val pw = it.next()
        if pw.future.toCompletableFuture.isDone then
          it.remove()

end CassandraSinkWriter
