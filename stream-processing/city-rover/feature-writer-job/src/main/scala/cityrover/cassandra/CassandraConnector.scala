package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.typesafe.config.Config

import java.net.InetSocketAddress

import cityrover.telemetry.{
  EnrichedTelemetryEvent,
  Window5sFeatures,
  Window30sFeatures,
  Window1mFeatures,
  Window5mFeatures
}


/**
  * CassandraConnector
  *
  * Wraps:
  *   - CqlSession
  *   - four per-window Prepared INSERT statements
  *
  * Binding is per-window: each helper names only that window's columns
  * and binds `USING TIMESTAMP ?` to `windowEndTs * 1000` (µs).
  *
  * No method binds columns it did not compute — that would produce
  * nulls, which Cassandra treats as tombstones and effectively deletes
  * the column.
  */
final class CassandraConnector(
  val session: CqlSession,
  val stmt5s: PreparedStatement,
  val stmt30s: PreparedStatement,
  val stmt1m: PreparedStatement,
  val stmt5m: PreparedStatement
):

  // ---------------------------------------------------------------------------
  // Bind helpers — one per window.
  //
  // `tsMicros` is `event.windowEndTs * 1000`, bound as the trailing
  // positional parameter of `USING TIMESTAMP ?`. Positional binding is
  // required for USING TIMESTAMP; named binding won't work for that slot.
  // ---------------------------------------------------------------------------

  def bind5s(e: EnrichedTelemetryEvent, f: Window5sFeatures, tsMicros: Long): BoundStatement =
    stmt5s.bind()
      .setString("rover_id", e.roverId)
      .setLong("updated_at_5s", e.windowEndTs)
      .setDouble("speed_avg_5s", f.speedAvg5S)
      .setDouble("speed_max_5s", f.speedMax5S)
      .setDouble("speed_min_5s", f.speedMin5S)
      .setDouble("acceleration_avg_5s", f.accelerationAvg5S)
      .setDouble("heading_change_5s", f.headingChange5S)
      .setDouble("distance_traveled_5s", f.distanceTraveled5S)
      .setLong("[timestamp]", tsMicros)   // ← see note below

  def bind30s(e: EnrichedTelemetryEvent, f: Window30sFeatures, tsMicros: Long): BoundStatement =
    stmt30s.bind()
      .setString("rover_id", e.roverId)
      .setLong("updated_at_30s", e.windowEndTs)
      .setDouble("speed_avg_30s", f.speedAvg30S)
      .setDouble("speed_std_30s", f.speedStd30S)
      .setDouble("acceleration_avg_30s", f.accelerationAvg30S)
      .setDouble("jerk_avg_30s", f.jerkAvg30S)
      .setDouble("turn_rate_avg_30s", f.turnRateAvg30S)
      .setInt("stops_count_30s", f.stopsCount30S)
      .setDouble("distance_traveled_30s", f.distanceTraveled30S)
      .setLong("[timestamp]", tsMicros)

  def bind1m(e: EnrichedTelemetryEvent, f: Window1mFeatures, tsMicros: Long): BoundStatement =
    stmt1m.bind()
      .setString("rover_id", e.roverId)
      .setLong("updated_at_1m", e.windowEndTs)
      .setDouble("speed_avg_1m", f.speedAvg1M)
      .setDouble("speed_std_1m", f.speedStd1M)
      .setDouble("acceleration_avg_1m", f.accelerationAvg1M)
      .setDouble("jerk_avg_1m", f.jerkAvg1M)
      .setDouble("turn_rate_avg_1m", f.turnRateAvg1M)
      .setDouble("idle_ratio_1m", f.idleRatio1M)
      .setDouble("distance_traveled_1m", f.distanceTraveled1M)
      .setDouble("congestion_level_1m", f.congestionLevel1M)
      .setLong("[timestamp]", tsMicros)

  def bind5m(e: EnrichedTelemetryEvent, f: Window5mFeatures, tsMicros: Long): BoundStatement =
    stmt5m.bind()
      .setString("rover_id", e.roverId)
      .setLong("updated_at_5m", e.windowEndTs)
      .setDouble("speed_avg_5m", f.speedAvg5M)
      .setDouble("speed_std_5m", f.speedStd5M)
      .setDouble("acceleration_avg_5m", f.accelerationAvg5M)
      .setDouble("jerk_avg_5m", f.jerkAvg5M)
      .setDouble("turn_rate_avg_5m", f.turnRateAvg5M)
      .setDouble("idle_ratio_5m", f.idleRatio5M)
      .setDouble("distance_traveled_5m", f.distanceTraveled5M)
      .setDouble("congestion_level_5m", f.congestionLevel5M)
      .setLong("[timestamp]", tsMicros)

  def close(): Unit =
    if session != null then session.close()

end CassandraConnector


object CassandraConnector:

  // ---------------------------------------------------------------------------
  // 1. Bootstrap session (NO keyspace) — used only to create schema
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
  // 3. Runtime connector (session + four per-window prepared statements)
  // ---------------------------------------------------------------------------
  def createRuntimeConnector(config: Config): CassandraConnector =
    val session = createRuntimeSession(config)
    val keyspace = config.getString("cityrover.cassandra.keyspace")
    val table    = config.getString("cityrover.cassandra.table")

    val stmt5s  = session.prepare(cql5s(keyspace, table))
    val stmt30s = session.prepare(cql30s(keyspace, table))
    val stmt1m  = session.prepare(cql1m(keyspace, table))
    val stmt5m  = session.prepare(cql5m(keyspace, table))

    CassandraConnector(session, stmt5s, stmt30s, stmt1m, stmt5m)

  // ---------------------------------------------------------------------------
  // Per-window CQL. Each names ONLY its own columns + the write timestamp.
  // ---------------------------------------------------------------------------

  private def cql5s(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_5s,
       |   speed_avg_5s, speed_max_5s, speed_min_5s,
       |   acceleration_avg_5s, heading_change_5s, distance_traveled_5s)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

  private def cql30s(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_30s,
       |   speed_avg_30s, speed_std_30s, acceleration_avg_30s,
       |   jerk_avg_30s, turn_rate_avg_30s, stops_count_30s,
       |   distance_traveled_30s)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

  private def cql1m(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_1m,
       |   speed_avg_1m, speed_std_1m, acceleration_avg_1m,
       |   jerk_avg_1m, turn_rate_avg_1m, idle_ratio_1m,
       |   distance_traveled_1m, congestion_level_1m)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

  private def cql5m(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_5m,
       |   speed_avg_5m, speed_std_5m, acceleration_avg_5m,
       |   jerk_avg_5m, turn_rate_avg_5m, idle_ratio_5m,
       |   distance_traveled_5m, congestion_level_5m)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

end CassandraConnector
