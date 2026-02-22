package phd.architecture.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.mutable.ListBuffer
import phd.architecture.model.WindowResult


class PostgresWindowResultSink(batchSize: Int = 200)
  extends RichSinkFunction[WindowResult] {

  @transient private var conn: Connection = _
  @transient private var stmt: PreparedStatement = _
  @transient private var buffer: ListBuffer[WindowResult] = _

  override def open(parameters: Configuration): Unit = {
    Class.forName("org.postgresql.Driver")

    conn = DriverManager.getConnection(
      "jdbc:postgresql://postgis-cre:5432/cre_db",
      "cre_user",
      "cre_password"
    )

    stmt = conn.prepareStatement(
      """
        |INSERT INTO cre.window_results (
        |  window_start,
        |  window_end,
        |  geohash,
        |  value,
        |  processing_time
        |) VALUES (?, ?, ?, ?, ?)
        |""".stripMargin
    )

    buffer = ListBuffer.empty[WindowResult]
  }

  override def invoke(result: WindowResult, context: SinkFunction.Context): Unit = {
    buffer += result
    if (buffer.size >= batchSize) {
      flushBatch()
    }
  }

  private def flushBatch(): Unit = {
    if (buffer.isEmpty) return

    try {
      buffer.foreach { r =>
        stmt.setLong(1, r.windowStart)
        stmt.setLong(2, r.windowEnd)
        stmt.setString(3, r.partition.geohash)
        stmt.setLong(4, r.value)
        
        r.processingTime match {
          case Some(time) => stmt.setObject(5, time)
          case None => stmt.setNull(5, java.sql.Types.TIMESTAMP)
        }
        
        stmt.addBatch()
      }

      stmt.executeBatch()
      stmt.clearParameters()
      
    } catch {
      case e: Exception =>
        println(s"Error flushing batch to PostgreSQL: ${e.getMessage}")
    } finally {
      buffer.clear()
    }
  }

  override def close(): Unit = {
    flushBatch()
    if (stmt != null) stmt.close()
    if (conn != null) conn.close()
  }
}
