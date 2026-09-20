package cityrover.cassandra

import cityrover.telemetry.EnrichedTelemetryEvent

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.typesafe.config.Config

import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

import java.util.ArrayList
import java.util.concurrent.CompletionStage


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

  private val pendingFutures =
    new ArrayList[CompletionStage[AsyncResultSet]]()

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
      CassandraSchema.initialize(
        bootstrapSession,
        keyspace,
        table
      )
    finally
      bootstrapSession.close()

    CassandraConnector.createRuntimeConnector(config)

  // ---------------------------------------------------------------------------
  // Write enriched telemetry events asynchronously
  // ---------------------------------------------------------------------------

  override def write(
    event: EnrichedTelemetryEvent,
    context: SinkWriter.Context
  ): Unit =
    try
      val stmt   = connector.bind(event)
      val future = connector.session.executeAsync(stmt)

      pendingFutures.synchronized:
        pendingFutures.add(future)

      cleanupCompletedFutures()

    catch
      case ex: Exception =>
        log.error(
          s"Failed to write enriched telemetry event to Cassandra: $event",
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
    pendingFutures.synchronized:
      if !pendingFutures.isEmpty then
        log.info(s"Waiting for ${pendingFutures.size} pending async writes...")

        try
          pendingFutures.forEach { future =>
            future.toCompletableFuture.join()
          }
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
        val future = it.next()
        if future.toCompletableFuture.isDone then
          it.remove()

end CassandraSinkWriter
