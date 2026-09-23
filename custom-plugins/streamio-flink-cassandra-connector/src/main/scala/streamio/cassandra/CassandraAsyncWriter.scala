package streamio.cassandra

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, BoundStatement}
import com.datastax.oss.driver.api.core.CqlSession
import org.slf4j.LoggerFactory

import java.util.concurrent.{CompletableFuture, CompletionStage}
import java.util.{ArrayList, List => JList}


/**
  * CassandraAsyncWriter
  *
  * Core async write engine shared by:
  *   - Table API sink (CassandraTableSinkWriter)
  *   - DataStream API sink (CassandraSinkWriter)
  *
  * Responsibilities:
  *   - async executeAsync() writes
  *   - inflight limit
  *   - batching (optional)
  *   - backpressure via futures
  *   - checkpoint-aware flush()
  *   - safe close()
  */
final class CassandraAsyncWriter(
  session: CqlSession,
  maxInflight: Int
):

  private val log = LoggerFactory.getLogger(getClass)

  /** Futures for inflight async writes */
  private val pending: JList[CompletionStage[AsyncResultSet]] =
    new ArrayList[CompletionStage[AsyncResultSet]]()

  /** Submit a BoundStatement asynchronously */
  def write(stmt: BoundStatement): Unit =
    try
      val future = session.executeAsync(stmt)

      pending.synchronized {
        pending.add(future)
      }

      cleanupCompleted()

      // Backpressure: if too many inflight writes, block until some complete
      if pending.size() >= maxInflight then
        waitForSome()
    catch
      case ex: Exception =>
        log.error("Failed to write BoundStatement to Cassandra", ex)
        throw ex

  /** Flush all pending writes (called by Flink checkpoint) */
  def flush(): Unit =
    log.debug(s"Flushing ${pending.size()} pending Cassandra writes")
    waitForAll()

  /** Close writer and session */
  def close(): Unit =
    try
      flush()
    catch
      case ex: Exception =>
        log.warn("Error while flushing during close()", ex)
    finally
      try session.close()
      catch
        case ex: Exception =>
          log.warn("Error closing Cassandra session", ex)

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /** Remove completed futures from the pending list */
  private def cleanupCompleted(): Unit =
    pending.synchronized {
      val it = pending.iterator()
      while it.hasNext do
        val f = it.next()
        if f.toCompletableFuture.isDone then
          it.remove()
    }

  /** Wait until at least one future completes */
  private def waitForSome(): Unit =
    pending.synchronized {
      if !pending.isEmpty then
        try
          // Wait for the first future to complete
          pending.get(0).toCompletableFuture.join()
        catch
          case ex: Exception =>
            log.error("Error waiting for inflight Cassandra write", ex)
        finally
          cleanupCompleted()
    }

  /** Wait for all pending futures */
  private def waitForAll(): Unit =
    pending.synchronized {
      if !pending.isEmpty then
        log.debug(s"Waiting for ${pending.size()} Cassandra writes to complete")
        try
          pending.forEach { f =>
            f.toCompletableFuture.join()
          }
        catch
          case ex: Exception =>
            log.error("Error waiting for Cassandra writes", ex)
        finally
          pending.clear()
    }

end CassandraAsyncWriter
