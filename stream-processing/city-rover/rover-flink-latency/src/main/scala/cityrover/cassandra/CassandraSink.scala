package cityrover.cassandra

import com.typesafe.config.Config
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.slf4j.LoggerFactory

import java.util.concurrent.{CompletableFuture, CompletionStage}
import scala.collection.mutable.ArrayBuffer

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
  private val pendingFutures = ArrayBuffer.empty[CompletionStage[_]]

  // ---------------------------------------------------------------------------
  // Initialize Cassandra connector with schema (bootstrap session)
  // ---------------------------------------------------------------------------
  private val connector: CassandraConnector =
    val config   = connectorConfig.config
    val keyspace = config.getString("cityrover.cassandra.keyspace")
    val table    = config.getString("cityrover.cassandra.table")

    // 1. Bootstrap schema using a session WITHOUT keyspace
    val bootstrapSession = CassandraConnector.createBootstrapSession(config)

    try
      CassandraSchema.initialize(bootstrapSession, keyspace, table)
    finally
      bootstrapSession.close()

    // 2. Runtime connector (session WITH keyspace + prepared statements)
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
        pendingFutures += future

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
      if pendingFutures.nonEmpty then
        log.info(s"Waiting for ${pendingFutures.size} pending async writes...")

        try
          val completableFutures =
            pendingFutures.map(_.toCompletableFuture).toArray
          CompletableFuture.allOf(completableFutures: _*).join()
        catch
          case ex: Exception =>
            log.error("Error while waiting for pending writes", ex)
        finally
          pendingFutures.clear()
      else
        log.debug("No pending writes to wait for")

  private def cleanupCompletedFutures(): Unit =
    pendingFutures.synchronized:
      pendingFutures.filterInPlace { future =>
        future.toCompletableFuture match
          case cf if cf.isDone => false   // drop completed
          case _               => true    // keep pending
      }

end CassandraSinkWriter
