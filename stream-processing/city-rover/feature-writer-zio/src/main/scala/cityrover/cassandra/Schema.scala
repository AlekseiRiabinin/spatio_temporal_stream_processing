package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import zio._
import org.slf4j.LoggerFactory


/** Creates the keyspace and `rover_features` table on startup.
  *
  * Called once per job with the bootstrap session (no keyspace bound).
  * Uses `CREATE ... IF NOT EXISTS` so it's idempotent across restarts.
  *
  * Schema design — one row per rover:
  *   - Each window's partial record upserts ONLY its own columns.
  *   - Cassandra's `INSERT` is an upsert: unmentioned columns are left
  *     untouched, so 5s / 30s / 1m / 5m writes merge into the same row.
  *   - The writer uses `USING TIMESTAMP = windowEndTs * 1000` (µs) so
  *     replays and reordering converge via last-write-wins.
  *   - Per-window `*_updated_at` columns carry the event-time boundary
  *     for each group, so readers can tell which group in a mixed-
  *     freshness row is stale.
  *
  * Compaction: SizeTieredCompactionStrategy is the default for
  * non-time-series tables. TimeWindowCompactionStrategy requires a
  * time-based clustering column in the PK, which we don't have
  * (one-row-per-rover, no clustering).
  */
object CassandraSchema:

  private val log = LoggerFactory.getLogger(getClass)

  /** Create the keyspace and table. Idempotent.
    *
    * @param session   bootstrap session (no keyspace bound)
    * @param keyspace  target keyspace name
    * @param table     target table name
    */
  def initialize(session: CqlSession, keyspace: String, table: String): Unit =
    log.info(s"Initializing Cassandra schema (keyspace: $keyspace, table: $table)...")

    // -------------------------------------------------------------------------
    // Keyspace
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
    // Table — one row per rover.
    //
    // `ts` is a regular column (NOT part of the PK): it holds the
    // event-time of the latest sampled event, updated by every window
    // write with the same `USING TIMESTAMP` as the window's own
    // columns. It converges to the newest snapshot without affecting
    // conflict resolution of feature columns.
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
         |    updated_at_5s          bigint,
         |
         |    speed_avg_30s          double,
         |    speed_std_30s          double,
         |    acceleration_avg_30s   double,
         |    jerk_avg_30s           double,
         |    turn_rate_avg_30s      double,
         |    stops_count_30s        int,
         |    distance_traveled_30s  double,
         |    updated_at_30s         bigint,
         |
         |    speed_avg_1m           double,
         |    speed_std_1m           double,
         |    acceleration_avg_1m    double,
         |    jerk_avg_1m            double,
         |    turn_rate_avg_1m       double,
         |    idle_ratio_1m          double,
         |    distance_traveled_1m   double,
         |    congestion_level_1m    double,
         |    updated_at_1m          bigint,
         |
         |    speed_avg_5m           double,
         |    speed_std_5m           double,
         |    acceleration_avg_5m    double,
         |    jerk_avg_5m            double,
         |    turn_rate_avg_5m       double,
         |    idle_ratio_5m          double,
         |    distance_traveled_5m   double,
         |    congestion_level_5m    double,
         |    updated_at_5m          bigint,
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
         |    PRIMARY KEY (rover_id)
         |) WITH compaction = {
         |      'class': 'SizeTieredCompactionStrategy'
         |  }
         |  AND default_time_to_live = 0;
         |""".stripMargin

    session.execute(tableCql)
    log.info(s"Table '$keyspace.$table' ensured.")

    log.info("Cassandra schema initialization complete.")
