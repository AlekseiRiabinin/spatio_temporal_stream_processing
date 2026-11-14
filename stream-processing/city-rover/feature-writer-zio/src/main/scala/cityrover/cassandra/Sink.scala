package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement}
import org.slf4j.LoggerFactory
import zio._

import cityrover.telemetry.{
  EnrichedTelemetryEvent,
  Window5sFeatures,
  Window30sFeatures,
  Window1mFeatures,
  Window5mFeatures
}
import cityrover.util.{AppConfig, CassandraConfig, CassandraDriverConfig}


/** Per-window Cassandra writes.
  *
  * Each window's partial record touches ONLY its own columns + the
  * shared key (`rover_id`) + `updated_at_<window>` + `USING TIMESTAMP`.
  * Cassandra's `INSERT` is an upsert: unmentioned columns are left
  * untouched, so the four windows merge into the same row.
  *
  * `USING TIMESTAMP ?` is bound to `windowEndTs * 1000` (µs) so replays
  * and reordering converge via last-write-wins. Duplicate writes with
  * the same key, columns, and timestamp are no-ops.
  *
  * No null binds for uncomputed columns — each prepared statement names
  * only the columns its window carries. Binding `null` in Cassandra
  * creates a tombstone (deletes the column), which would erase data
  * written by another window.
  */
final class CassandraSink(
  session: CqlSession,
  cfg: CassandraConfig
):

  private val log = LoggerFactory.getLogger(getClass)

  import CassandraSink.*

  // ---------------------------------------------------------------------------
  // Prepare the four per-window statements once at construction.
  // ---------------------------------------------------------------------------

  private val stmt5s:  PreparedStatement = session.prepare(cql5s(cfg.keyspace, cfg.table))
  private val stmt30s: PreparedStatement = session.prepare(cql30s(cfg.keyspace, cfg.table))
  private val stmt1m:  PreparedStatement = session.prepare(cql1m(cfg.keyspace, cfg.table))
  private val stmt5m:  PreparedStatement = session.prepare(cql5m(cfg.keyspace, cfg.table))

  /** Write one enriched record asynchronously.
    *
    * `executeAsync` returns a `CompletionStage` that completes when the
    * Cassandra node acknowledges the write (or fails). ZIO backpressure
    * comes from `ZIO.fromCompletionStage` — if Cassandra is slow, the
    * ZIO effect doesn't complete, and ZIO Streams upstream demand
    * throttles naturally. No `Semaphore` needed (unlike the Flink sink).
    */
  def write(proto: EnrichedTelemetryEvent): Task[Unit] =
    if proto.roverId.isEmpty then
      ZIO.fail(new IllegalArgumentException("EnrichedTelemetryEvent.roverId is empty"))
    else
      val tsMicros = proto.windowEndTs * 1000L

      val bound: BoundStatement =
        proto.features match
          case EnrichedTelemetryEvent.Features.W5S(f)  => bind5s(proto, f, tsMicros)
          case EnrichedTelemetryEvent.Features.W30S(f) => bind30s(proto, f, tsMicros)
          case EnrichedTelemetryEvent.Features.W1M(f)  => bind1m(proto, f, tsMicros)
          case EnrichedTelemetryEvent.Features.W5M(f)  => bind5m(proto, f, tsMicros)
          case EnrichedTelemetryEvent.Features.Empty   =>
            throw new IllegalStateException(
              s"Empty features oneof for roverId='${proto.roverId}', windowType=${proto.windowType}"
            )

      ZIO
        .fromCompletionStage(session.executeAsync(bound).toCompletableFuture)
        .unit
        .tapError(e =>
          ZIO.logErrorCause(
            "Cassandra write failed: " +
            s"roverId=${proto.roverId}, " +
            s"windowType=${proto.windowType}, " +
            s"windowEndTs=${proto.windowEndTs}",
            Cause.fail(e)
          )
        )

  // ---------------------------------------------------------------------------
  // Bind helpers — one per window, all positional.
  //
  // Positional binding is required for USING TIMESTAMP: it's a trailing
  // `?` that has no column name. The named-binding API (setLong("col", v))
  // cannot address it. Every bind* uses positional indices 0..N throughout.
  //
  // Each cql*() below names the exact number of `?` placeholders; the
  // bind* helpers must match that count (N columns + 1 timestamp = N+1
  // placeholders).
  // ---------------------------------------------------------------------------

  private def bind5s(
    e: EnrichedTelemetryEvent, f: Window5sFeatures, tsMicros: Long
  ): BoundStatement =
    val b = stmt5s.bind()
    b.setString(0, e.roverId)
    b.setLong(1,   e.windowEndTs)         // updated_at_5s
    b.setDouble(2, f.speedAvg5S)
    b.setDouble(3, f.speedMax5S)
    b.setDouble(4, f.speedMin5S)
    b.setDouble(5, f.accelerationAvg5S)
    b.setDouble(6, f.headingChange5S)
    b.setDouble(7, f.distanceTraveled5S)
    b.setLong(8,   tsMicros)              // USING TIMESTAMP ?
    b

  private def bind30s(
    e: EnrichedTelemetryEvent, f: Window30sFeatures, tsMicros: Long
  ): BoundStatement =
    val b = stmt30s.bind()
    b.setString(0, e.roverId)
    b.setLong(1,   e.windowEndTs)         // updated_at_30s
    b.setDouble(2, f.speedAvg30S)
    b.setDouble(3, f.speedStd30S)
    b.setDouble(4, f.accelerationAvg30S)
    b.setDouble(5, f.jerkAvg30S)
    b.setDouble(6, f.turnRateAvg30S)
    b.setInt(7,    f.stopsCount30S)
    b.setDouble(8, f.distanceTraveled30S)
    b.setLong(9,   tsMicros)              // USING TIMESTAMP ?
    b

  private def bind1m(
    e: EnrichedTelemetryEvent, f: Window1mFeatures, tsMicros: Long
  ): BoundStatement =
    val b = stmt1m.bind()
    b.setString(0, e.roverId)
    b.setLong(1,   e.windowEndTs)         // updated_at_1m
    b.setDouble(2, f.speedAvg1M)
    b.setDouble(3, f.speedStd1M)
    b.setDouble(4, f.accelerationAvg1M)
    b.setDouble(5, f.jerkAvg1M)
    b.setDouble(6, f.turnRateAvg1M)
    b.setDouble(7, f.idleRatio1M)
    b.setDouble(8, f.distanceTraveled1M)
    b.setDouble(9, f.congestionLevel1M)
    b.setLong(10,  tsMicros)              // USING TIMESTAMP ?
    b

  private def bind5m(
    e: EnrichedTelemetryEvent, f: Window5mFeatures, tsMicros: Long
  ): BoundStatement =
    val b = stmt5m.bind()
    b.setString(0, e.roverId)
    b.setLong(1,   e.windowEndTs)         // updated_at_5m
    b.setDouble(2, f.speedAvg5M)
    b.setDouble(3, f.speedStd5M)
    b.setDouble(4, f.accelerationAvg5M)
    b.setDouble(5, f.jerkAvg5M)
    b.setDouble(6, f.turnRateAvg5M)
    b.setDouble(7, f.idleRatio5M)
    b.setDouble(8, f.distanceTraveled5M)
    b.setDouble(9, f.congestionLevel5M)
    b.setLong(10,  tsMicros)              // USING TIMESTAMP ?
    b


object CassandraSink:

  /** Build a `CassandraSink` from a provided `CqlSession` and config.
    *
    * The session is provided as a layer dependency (see
    * `CassandraSession.runtime`); the sink holds it for its lifetime
    * and does NOT close it (the layer's scope owns that).
    */
  val layer: ZLayer[AppConfig & CqlSession, Throwable, CassandraSink] =
    ZLayer.fromZIO {
      for
        cfg     <- ZIO.service[AppConfig]
        session <- ZIO.service[CqlSession]
      yield new CassandraSink(session, cfg.cassandra)
    }

  // ---------------------------------------------------------------------------
  // Per-window CQL. Each names ONLY its own columns + the key + the
  // updated_at_<window> marker + USING TIMESTAMP.
  // ---------------------------------------------------------------------------

  private[cassandra] def cql5s(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_5s,
       |   speed_avg_5s, speed_max_5s, speed_min_5s,
       |   acceleration_avg_5s, heading_change_5s, distance_traveled_5s)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

  private[cassandra] def cql30s(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_30s,
       |   speed_avg_30s, speed_std_30s, acceleration_avg_30s,
       |   jerk_avg_30s, turn_rate_avg_30s, stops_count_30s,
       |   distance_traveled_30s)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

  private[cassandra] def cql1m(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_1m,
       |   speed_avg_1m, speed_std_1m, acceleration_avg_1m,
       |   jerk_avg_1m, turn_rate_avg_1m, idle_ratio_1m,
       |   distance_traveled_1m, congestion_level_1m)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin

  private[cassandra] def cql5m(ks: String, tbl: String): String =
    s"""INSERT INTO $ks.$tbl
       |  (rover_id, updated_at_5m,
       |   speed_avg_5m, speed_std_5m, acceleration_avg_5m,
       |   jerk_avg_5m, turn_rate_avg_5m, idle_ratio_5m,
       |   distance_traveled_5m, congestion_level_5m)
       |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       |USING TIMESTAMP ?""".stripMargin
