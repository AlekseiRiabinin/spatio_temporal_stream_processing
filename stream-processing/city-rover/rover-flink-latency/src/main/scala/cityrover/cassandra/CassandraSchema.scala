package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import org.slf4j.LoggerFactory


object CassandraSchema:

  private val log = LoggerFactory.getLogger(getClass)

  def initialize(session: CqlSession, keyspace: String, table: String): Unit =
    log.info(s"Initializing Cassandra schema (keyspace: $keyspace, table: $table)...")

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

    val tableCql =
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$table (
         |    rover_id    text,
         |    ts          bigint,
         |    lat         double,
         |    lon         double,
         |    speed       double,
         |    heading     double,
         |    latency_ns  bigint,
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
