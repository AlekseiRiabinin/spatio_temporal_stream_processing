package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.typesafe.config.Config

import java.net.InetSocketAddress


final class CassandraConnector(
  val session: CqlSession,
  val insertTelemetryStmt: PreparedStatement
):

  /** Bind values to the prepared statement */
  def bindTelemetry(
    roverId: String,
    ts: Long,
    lat: Double,
    lon: Double,
    speed: Double,
    heading: Double,
    latencyNs: Long
  ): BoundStatement =
    insertTelemetryStmt.bind(
      roverId,
      Long.box(ts),
      Double.box(lat),
      Double.box(lon),
      Double.box(speed),
      Double.box(heading),
      Long.box(latencyNs)
    )

  /** Close session when shutting down the Flink job */
  def close(): Unit =
    if session != null then session.close()

end CassandraConnector


object CassandraConnector:

  // ---------------------------------------------------------------------------
  // 1. Bootstrap session (NO keyspace)
  // ---------------------------------------------------------------------------
  def createBootstrapSession(config: Config): CqlSession =
    val host = config.getString("cityrover.cassandra.host")
    val port = config.getInt("cityrover.cassandra.port")

    CqlSession.builder()
      .addContactPoint(InetSocketAddress(host, port))
      .withLocalDatacenter("datacenter1")
      .build()

  // ---------------------------------------------------------------------------
  // 2. Runtime session (WITH keyspace)
  // ---------------------------------------------------------------------------
  def createRuntimeSession(config: Config): CqlSession =
    val host     = config.getString("cityrover.cassandra.host")
    val port     = config.getInt("cityrover.cassandra.port")
    val keyspace = config.getString("cityrover.cassandra.keyspace")

    CqlSession.builder()
      .addContactPoint(InetSocketAddress(host, port))
      .withLocalDatacenter("datacenter1")
      .withKeyspace(keyspace)
      .build()

  // ---------------------------------------------------------------------------
  // 3. Runtime connector (session + prepared statement)
  // ---------------------------------------------------------------------------
  def createRuntimeConnector(config: Config): CassandraConnector =
    val session = createRuntimeSession(config)
    val table   = config.getString("cityrover.cassandra.table")

    val insertStmt = session.prepare(
      s"""
         |INSERT INTO $table (
         |  rover_id,
         |  ts,
         |  lat,
         |  lon,
         |  speed,
         |  heading,
         |  latency_ns
         |) VALUES (?, ?, ?, ?, ?, ?, ?)
         |""".stripMargin
    )

    CassandraConnector(session, insertStmt)

end CassandraConnector
