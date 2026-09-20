package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import org.slf4j.LoggerFactory


object CassandraSchema:

  private val log = LoggerFactory.getLogger(getClass)

  def initialize(session: CqlSession, keyspace: String, table: String): Unit =
    log.info(s"Initializing Cassandra schema (keyspace: $keyspace, table: $table)...")

    // -------------------------------------------------------------------------
    // Create keyspace
    // -------------------------------------------------------------------------
    val keyspaceCql =
      s"""
         |CREATE KEYSPACE IF NOT EXISTS $keyspace
         |WITH replication = {
         |  'class': 'SimpleStrategy',
         |  'replication_factor': 1
         |};
         |""".stripMargin

    session.execute(keyspaceCql)
    log.info(s"Keyspace '$keyspace' ensured.")

    // -------------------------------------------------------------------------
    // Create enriched telemetry table
    // -------------------------------------------------------------------------
    val tableCql =
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table (
         |    rover_id               text,
         |    ts                     bigint,
         |
         |    edge_id                text,
         |    route_id               text,
         |
         |    lat                    double,
         |    lon                    double,
         |    speed                  double,
         |    heading                double,
         |
         |    speed_avg_5s           double,
         |    speed_max_5s           double,
         |    speed_min_5s           double,
         |    acceleration_avg_5s    double,
         |    heading_change_5s      double,
         |    distance_traveled_5s   double,
         |
         |    speed_avg_30s          double,
         |    speed_std_30s          double,
         |    acceleration_avg_30s   double,
         |    jerk_avg_30s           double,
         |    turn_rate_avg_30s      double,
         |    stops_count_30s        int,
         |    distance_traveled_30s  double,
         |
         |    speed_avg_1m           double,
         |    speed_std_1m           double,
         |    acceleration_avg_1m    double,
         |    jerk_avg_1m            double,
         |    turn_rate_avg_1m       double,
         |    idle_ratio_1m          double,
         |    distance_traveled_1m   double,
         |    congestion_level_1m    double,
         |
         |    speed_avg_5m           double,
         |    speed_std_5m           double,
         |    acceleration_avg_5m    double,
         |    jerk_avg_5m            double,
         |    turn_rate_avg_5m       double,
         |    idle_ratio_5m          double,
         |    distance_traveled_5m   double,
         |    congestion_level_5m    double,
         |
         |    grid_cell_id           bigint,
         |    region_id              text,
         |    snapped_edge_id        text,
         |    snapped_lat            double,
         |    snapped_lon            double,
         |
         |    route_progress         double,
         |    route_deviation        double,
         |    expected_speed         double,
         |    speed_ratio            double,
         |
         |    driving_style_score    double,
         |    anomaly_score          double,
         |
         |    feature_version        text,
         |    feature_timestamp      bigint,
         |    feature_latency_ms     bigint,
         |
         |    is_outlier             boolean,
         |    is_gps_jump            boolean,
         |    is_speed_anomaly       boolean,
         |    is_heading_anomaly     boolean,
         |
         |    raw_event_hash         text,
         |    processing_node        text,
         |    processing_time_ms     bigint,
         |
         |    PRIMARY KEY (rover_id, ts)
         |) WITH CLUSTERING ORDER BY (ts DESC)
         |  AND compaction = {
         |      'class': 'TimeWindowCompactionStrategy',
         |      'compaction_window_size': '1',
         |      'compaction_window_unit': 'DAYS'
         |  }
         |  AND default_time_to_live = 0;
         |""".stripMargin

    session.execute(tableCql)
    log.info(s"Table '$keyspace.$table' ensured.")

    log.info("Cassandra schema initialization complete.")

end CassandraSchema
