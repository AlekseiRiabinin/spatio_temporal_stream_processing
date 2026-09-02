package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{PreparedStatement, BoundStatement}
import com.typesafe.config.Config

import java.net.InetSocketAddress


/**
  * CassandraConnector is responsible for:
  *   - Creating a CqlSession
  *   - Preparing statements
  *   - Providing access to the session for sinks
  *
  * It is intentionally lightweight and safe to use inside Flink operators.
  */
final class CassandraConnector(
    val session: CqlSession,
    val insertTelemetryStmt: PreparedStatement
) {

  /** Bind values to the prepared statement */
  def bindTelemetry(
    roverId: String,
    ts: Long,
    lat: Double,
    lon: Double,
    speed: Double,
    heading: Double
  ): BoundStatement =
    insertTelemetryStmt.bind(roverId, Long.box(ts), Double.box(lat), Double.box(lon), Double.box(speed), Double.box(heading))

  /** Close session when shutting down the Flink job */
  def close(): Unit =
    if (session != null) session.close()
}

object CassandraConnector {

  /**
    * Build connector from Typesafe Config.
    *
    * Expected config:
    *
    * cityrover.cassandra {
    *   host = "cassandra"
    *   port = 9042
    *   keyspace = "cityrover"
    *   table = "telemetry"
    * }
    */
  def fromConfig(config: Config): CassandraConnector = {

    val host     = config.getString("cityrover.cassandra.host")
    val port     = config.getInt("cityrover.cassandra.port")
    val keyspace = config.getString("cityrover.cassandra.keyspace")
    val table    = config.getString("cityrover.cassandra.table")

    val session =
      CqlSession.builder()
        .addContactPoint(new InetSocketAddress(host, port))
        .withLocalDatacenter("datacenter1") // Cassandra default DC name
        .withKeyspace(keyspace)
        .build()

    val insertStmt =
      session.prepare(
        s"""
           |INSERT INTO $table (
           |  rover_id,
           |  ts,
           |  lat,
           |  lon,
           |  speed,
           |  heading
           |) VALUES (?, ?, ?, ?, ?, ?)
           |""".stripMargin
      )

    new CassandraConnector(session, insertStmt)
  }
}
