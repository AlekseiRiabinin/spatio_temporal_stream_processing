package streamio.cassandra

import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import org.apache.flink.table.data._
import org.apache.flink.table.types.logical._


/**
  * CassandraRecordEncoder
  *
  * Converts Flink RowData → Cassandra BoundStatement.
  *
  * Used by:
  *   - CassandraTableSinkWriter (Table API)
  *   - CassandraSinkWriter (DataStream API)
  *
  * Responsibilities:
  *   - read RowData fields by index
  *   - convert Flink logical types to Cassandra Java types
  *   - bind values to PreparedStatement
  *
  * This encoder is stateless and reusable.
  */
final class CassandraRecordEncoder(
  prepared: PreparedStatement,
  fieldTypes: Array[LogicalType]
):

  /** Encode a RowData into a BoundStatement */
  def encode(row: RowData): BoundStatement =
    var stmt = prepared.bind()

    var i = 0
    while i < fieldTypes.length do
      val t = fieldTypes(i)

      stmt = t match
        case _: IntType =>
          stmt.setInt(i, row.getInt(i))

        case _: BigIntType =>
          stmt.setLong(i, row.getLong(i))

        case _: FloatType =>
          stmt.setFloat(i, row.getFloat(i))

        case _: DoubleType =>
          stmt.setDouble(i, row.getDouble(i))

        case _: BooleanType =>
          stmt.setBoolean(i, row.getBoolean(i))

        case _: VarCharType =>
          stmt.setString(i, row.getString(i).toString)

        case _: BinaryType | _: VarBinaryType =>
          stmt.setByteBuffer(i, java.nio.ByteBuffer.wrap(row.getBinary(i)))

        case _: TimestampType =>
          val ts = row.getTimestamp(i, t.asInstanceOf[TimestampType].getPrecision)
          stmt.setInstant(i, ts.toInstant)

        case other =>
          throw new UnsupportedOperationException(
            s"Unsupported Flink → Cassandra type: $other at index $i"
          )

      i += 1

    stmt

end CassandraRecordEncoder
