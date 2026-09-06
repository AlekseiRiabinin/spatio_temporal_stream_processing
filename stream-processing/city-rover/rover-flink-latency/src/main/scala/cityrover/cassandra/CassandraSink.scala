package cityrover.cassandra

import com.typesafe.config.Config
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.{ArrayList, List}

import cityrover.telemetry.EnrichedTelemetryEvent


case class CassandraConnectorConfig(config: Config)


final class CassandraSink(
  connectorConfig: CassandraConnectorConfig
) extends Sink[EnrichedTelemetryEvent]:

  override def createWriter(
    context: WriterInitContext
  ): SinkWriter[EnrichedTelemetryEvent] =
    new CassandraSinkWriter(connectorConfig)

end CassandraSink


final class CassandraSinkWriter(
  connectorConfig: CassandraConnectorConfig
) extends SinkWriter[EnrichedTelemetryEvent]:

  private val log = LoggerFactory.getLogger(getClass)

  private val pendingFutures: List[CompletionStage[_]] =
    new ArrayList[CompletionStage[_]]()

  // ---------------------------------------------------------------------------
  // Initialize Cassandra connector with schema (bootstrap session)
  // ---------------------------------------------------------------------------
  private val connector: CassandraConnector =
    val config   = connectorConfig.config
    val keyspace = config.getString("cityrover.cassandra.keyspace")
    val table    = config.getString("cityrover.cassandra.table")

    val bootstrapSession = CassandraConnector.createBootstrapSession(config)

    try
      CassandraSchema.initialize(bootstrapSession, keyspace, table)

    finally
      bootstrapSession.close()

    CassandraConnector.createRuntimeConnector(config)

  // ---------------------------------------------------------------------------
  // Write telemetry events asynchronously
  // ---------------------------------------------------------------------------
  override def write(
    event: EnrichedTelemetryEvent,
    context: SinkWriter.Context
  ): Unit =
    try
      val stmt = connector.bindTelemetry(
        event.roverId,
        event.ts,
        event.lat,
        event.lon,
        event.speed,
        event.heading,
        event.latencyNs
      )

      val future = connector.session.executeAsync(stmt)

      pendingFutures.synchronized:
        pendingFutures.add(future)

      cleanupCompletedFutures()

    catch
      case ex: Exception =>
        log.error(s"Failed to write telemetry event to Cassandra: $event", ex)

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
    pendingFutures.synchronized:
      if !pendingFutures.isEmpty then
        log.info(s"Waiting for ${pendingFutures.size} pending async writes...")

        try
          // Convert Java List → Array[CompletableFuture[_]]
          val cfArray: Array[CompletableFuture[_]] =
            pendingFutures.stream()
              .map(_.toCompletableFuture)
              .toArray(size => new Array[CompletableFuture[_]](size))

          CompletableFuture.allOf(cfArray: _*).join()

        catch
          case ex: Exception =>
            log.error("Error while waiting for pending writes", ex)

        finally
          pendingFutures.clear()

      else
        log.debug("No pending writes to wait for")

  private def cleanupCompletedFutures(): Unit =
    pendingFutures.synchronized:
      val it = pendingFutures.iterator()
      while it.hasNext do
        val cf = it.next().toCompletableFuture
        if cf.isDone then it.remove()

end CassandraSinkWriter
