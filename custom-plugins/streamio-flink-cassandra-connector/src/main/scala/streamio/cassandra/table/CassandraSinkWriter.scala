package streamio.cassandra.table

import streamio.cassandra.{
  CassandraSessionFactory,
  CassandraRecordEncoder,
  CassandraAsyncWriter
}

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement

import org.apache.flink.api.connector.sink2.{SinkWriter, WriterInitContext}
import org.apache.flink.api.connector.sink2.SinkWriter.Context
import org.apache.flink.table.types.logical.{RowType, LogicalType}
import org.apache.flink.table.data.RowData

import org.slf4j.LoggerFactory

import java.util


/**
  * CassandraSinkWriter
  *
  * Flink Sink V2 writer for Cassandra.
  *
  * Responsibilities:
  *   - create Cassandra session from connector options
  *   - prepare INSERT statement based on RowType
  *   - encode RowData → BoundStatement via CassandraRecordEncoder
  *   - delegate async writes to CassandraAsyncWriter
  *   - implement write(), flush(), close() for Sink V2
  */
final class CassandraSinkWriter(
  rowType: RowType,
  options: util.Map[String, String],
  initContext: WriterInitContext
) extends SinkWriter[RowData]:

  private val log = LoggerFactory.getLogger(getClass)

  // ---------------------------------------------------------------------------
  // Options
  // ---------------------------------------------------------------------------

  private val keyspace: String =
    options.getOrDefault("keyspace", "default_keyspace")

  private val table: String =
    options.getOrDefault("table", "default_table")

  private val maxInflight: Int =
    Option(options.get("maxInflight")).map(_.toInt).getOrElse(1024)

  // ---------------------------------------------------------------------------
  // Cassandra session + prepared statement
  // ---------------------------------------------------------------------------

  private val session: CqlSession =
    CassandraSessionFactory.fromOptions(options)

  private val insertCql: String =
    s"INSERT INTO $keyspace.$table (${columnList(rowType)}) VALUES (${placeholders(rowType)})"

  private val prepared: PreparedStatement =
    session.prepare(insertCql)

  // ---------------------------------------------------------------------------
  // Encoder + async writer
  // ---------------------------------------------------------------------------

  private val fieldTypes: Array[LogicalType] =
    rowType.getFields
      .toArray(new Array[RowType.RowField](rowType.getFieldCount))
      .map(_.getType)

  private val encoder =
    new CassandraRecordEncoder(prepared, fieldTypes)

  private val asyncWriter =
    new CassandraAsyncWriter(session, maxInflight)

  // ---------------------------------------------------------------------------
  // Write RowData → Cassandra
  // ---------------------------------------------------------------------------

  override def write(
    row: RowData,
    context: Context
  ): Unit =
    try
      val stmt = encoder.encode(row)
      asyncWriter.write(stmt)
    catch
      case ex: Exception =>
        log.error(s"Failed to write RowData to Cassandra: $row", ex)
        throw ex

  // ---------------------------------------------------------------------------
  // Flush pending writes (checkpoint)
  // ---------------------------------------------------------------------------

  override def flush(endOfInput: Boolean): Unit =
    asyncWriter.flush()

  // ---------------------------------------------------------------------------
  // Close writer + session
  // ---------------------------------------------------------------------------

  override def close(): Unit =
    try asyncWriter.close()
    catch
      case ex: Exception =>
        log.warn("Error while closing CassandraAsyncWriter", ex)

  // ---------------------------------------------------------------------------
  // Helpers: build CQL from RowType
  // ---------------------------------------------------------------------------

  private def columnList(rt: RowType): String =
    String.join(", ", rt.getFieldNames)

  private def placeholders(rt: RowType): String =
    String.join(", ", java.util.Collections.nCopies(rt.getFieldCount, "?"))

end CassandraSinkWriter
