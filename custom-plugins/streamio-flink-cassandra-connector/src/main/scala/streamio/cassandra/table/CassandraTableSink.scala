package streamio.cassandra.table

import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider
import org.apache.flink.table.connector.sink.SinkV2Provider
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.table.data.RowData
import org.apache.flink.types.RowKind

import java.util


/**
  * CassandraTableSink
  *
  * Bridges Flink Table API → Cassandra Sink V2.
  *
  * Responsibilities:
  *   - hold schema + connector options
  *   - declare supported changelog mode (INSERT only)
  *   - create SinkV2Provider for CassandraSink
  */
final class CassandraTableSink(
  schema: ResolvedSchema,
  options: util.Map[String, String]
) extends DynamicTableSink:

  // ---------------------------------------------------------------------------
  // Changelog mode (INSERT-only)
  // ---------------------------------------------------------------------------

  override def getChangelogMode(requestedMode: ChangelogMode): ChangelogMode =
    ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .build()

  // ---------------------------------------------------------------------------
  // Runtime provider (Sink V2)
  // ---------------------------------------------------------------------------

  override def getSinkRuntimeProvider(
    context: DynamicTableSink.Context
  ): SinkRuntimeProvider =
    val rowType =
      schema.toPhysicalRowDataType.getLogicalType.asInstanceOf[RowType]

    SinkV2Provider.of(
      new CassandraSink(rowType, options)
    )

  // ---------------------------------------------------------------------------
  // Copy
  // ---------------------------------------------------------------------------

  override def copy(): DynamicTableSink =
    new CassandraTableSink(schema, options)

  // ---------------------------------------------------------------------------
  // Summary string
  // ---------------------------------------------------------------------------

  override def asSummaryString(): String =
    "CassandraTableSink"

end CassandraTableSink
