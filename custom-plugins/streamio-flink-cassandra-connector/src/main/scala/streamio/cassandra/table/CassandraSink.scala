package streamio.cassandra.table

import org.apache.flink.api.connector.sink2.{Sink, SinkWriter, WriterInitContext}
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.data.RowData

import java.util


/**
  * CassandraSink
  *
  * Flink Sink V2 entry point for Cassandra.
  *
  * Responsibilities:
  *   - hold physical RowType + connector options
  *   - provide a serializable Sink instance for task deployment
  *   - create CassandraSinkWriter for RowData ingestion
  *
  * The actual write logic (session, prepared statement, encoding, async I/O)
  * is implemented inside CassandraSinkWriter.
  */
@SerialVersionUID(1L)
final class CassandraSink(
  rowType: RowType,
  options: util.Map[String, String]
) extends Sink[RowData]:

  private val serializableOptions: util.HashMap[String, String] =
    new util.HashMap[String, String](options)

  override def createWriter(context: WriterInitContext): SinkWriter[RowData] =
    new CassandraSinkWriter(rowType, serializableOptions, context)

end CassandraSink
