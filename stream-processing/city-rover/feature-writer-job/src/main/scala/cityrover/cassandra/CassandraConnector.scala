package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import com.typesafe.config.Config

import java.net.InetSocketAddress
import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * CassandraConnector
  *
  * Wraps:
  *   - CqlSession
  *   - Prepared INSERT statement
  *
  * Provides bind() for EnrichedTelemetryEvent → BoundStatement.
  */
final class CassandraConnector(
  val session: CqlSession,
  val insertStmt: PreparedStatement
):

  /** Bind all enriched telemetry fields to the prepared statement */
  def bind(event: EnrichedTelemetryEvent): BoundStatement =
    insertStmt.bind(
      event.roverId,
      event.edgeId,
      event.routeId,

      // Raw telemetry
      Double.box(event.lat),
      Double.box(event.lon),
      Long.box(event.ts),
      Double.box(event.speed),
      Double.box(event.heading),

      // 5-second window features
      Double.box(event.speedAvg5s),
      Double.box(event.speedMax5s),
      Double.box(event.speedMin5s),
      Double.box(event.accelerationAvg5s),
      Double.box(event.headingChange5s),
      Double.box(event.distanceTraveled5s),

      // 30-second window features
      Double.box(event.speedAvg30s),
      Double.box(event.speedStd30s),
      Double.box(event.accelerationAvg30s),
      Double.box(event.jerkAvg30s),
      Double.box(event.turnRateAvg30s),
      Int.box(event.stopsCount30s),
      Double.box(event.distanceTraveled30s),

      // 1-minute window features
      Double.box(event.speedAvg1m),
      Double.box(event.speedStd1m),
      Double.box(event.accelerationAvg1m),
      Double.box(event.jerkAvg1m),
      Double.box(event.turnRateAvg1m),
      Double.box(event.idleRatio1m),
      Double.box(event.distanceTraveled1m),
      Double.box(event.congestionLevel1m),

      // 5-minute window features
      Double.box(event.speedAvg5m),
      Double.box(event.speedStd5m),
      Double.box(event.accelerationAvg5m),
      Double.box(event.jerkAvg5m),
      Double.box(event.turnRateAvg5m),
      Double.box(event.idleRatio5m),
      Double.box(event.distanceTraveled5m),
      Double.box(event.congestionLevel5m),

      // Geospatial features
      Long.box(event.gridCellId),
      event.regionId,
      event.snappedEdgeId,
      Double.box(event.snappedLat),
      Double.box(event.snappedLon),

      // Route / navigation features
      Double.box(event.routeProgress),
      Double.box(event.routeDeviation),
      Double.box(event.expectedSpeed),
      Double.box(event.speedRatio),

      // Behavioral features
      Double.box(event.drivingStyleScore),
      Double.box(event.anomalyScore),

      // ML metadata
      event.featureVersion,
      Long.box(event.featureTimestamp),
      Long.box(event.featureLatencyMs),

      // Quality / anomaly detection
      Boolean.box(event.isOutlier),
      Boolean.box(event.isGpsJump),
      Boolean.box(event.isSpeedAnomaly),
      Boolean.box(event.isHeadingAnomaly),

      // Debug / observability
      event.rawEventHash,
      event.processingNode,
      Long.box(event.processingTimeMs)
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
         |  edge_id,
         |  route_id,
         |
         |  lat, lon, ts, speed, heading,
         |
         |  speed_avg_5s, speed_max_5s, speed_min_5s,
         |  acceleration_avg_5s, heading_change_5s, distance_traveled_5s,
         |
         |  speed_avg_30s, speed_std_30s, acceleration_avg_30s,
         |  jerk_avg_30s, turn_rate_avg_30s, stops_count_30s,
         |  distance_traveled_30s,
         |
         |  speed_avg_1m, speed_std_1m, acceleration_avg_1m,
         |  jerk_avg_1m, turn_rate_avg_1m, idle_ratio_1m,
         |  distance_traveled_1m, congestion_level_1m,
         |
         |  speed_avg_5m, speed_std_5m, acceleration_avg_5m,
         |  jerk_avg_5m, turn_rate_avg_5m, idle_ratio_5m,
         |  distance_traveled_5m, congestion_level_5m,
         |
         |  grid_cell_id, region_id, snapped_edge_id,
         |  snapped_lat, snapped_lon,
         |
         |  route_progress, route_deviation, expected_speed, speed_ratio,
         |
         |  driving_style_score, anomaly_score,
         |
         |  feature_version, feature_timestamp, feature_latency_ms,
         |
         |  is_outlier, is_gps_jump, is_speed_anomaly, is_heading_anomaly,
         |
         |  raw_event_hash, processing_node, processing_time_ms
         |) VALUES (
         |  ?, ?, ?,
         |  ?, ?, ?, ?, ?,
         |  ?, ?, ?, ?, ?, ?,
         |  ?, ?, ?, ?, ?, ?, ?,
         |  ?, ?, ?, ?, ?, ?, ?, ?,
         |  ?, ?, ?, ?, ?, ?, ?, ?,
         |  ?, ?, ?, ?, ?,
         |  ?, ?, ?, ?,
         |  ?, ?,
         |  ?, ?, ?,
         |  ?, ?, ?, ?,
         |  ?, ?, ?
         |)
         |""".stripMargin
    )

    CassandraConnector(session, insertStmt)

end CassandraConnector
