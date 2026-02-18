package phd.architecture.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import java.sql.{Connection, DriverManager, PreparedStatement}
import phd.architecture.model.WindowResult


class PostgresWindowResultSink extends RichSinkFunction[WindowResult] {

  @transient private var conn: Connection = _
  @transient private var stmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    Class.forName("org.postgresql.Driver")

    conn = DriverManager.getConnection(
      "jdbc:postgresql://postgis-cre:5432/cre_db",
      "cre_user",
      "cre_password"
    )

    stmt = conn.prepareStatement(
      """
        |INSERT INTO window_results (
        |  window_start,
        |  window_end,
        |  geohash,
        |  value,
        |  processing_time
        |) VALUES (?, ?, ?, ?, ?)
        |""".stripMargin
    )
  }

  override def invoke(result: WindowResult, context: SinkFunction.Context): Unit = {
    stmt.setLong(1, result.windowStart)
    stmt.setLong(2, result.windowEnd)
    stmt.setString(3, result.partition.geohash)
    stmt.setLong(4, result.value)
    stmt.setObject(5, result.processingTime.orNull)

    stmt.executeUpdate()
  }

  override def close(): Unit = {
    if (stmt != null) stmt.close()
    if (conn != null) conn.close()
  }
}
