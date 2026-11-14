package cityrover.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import zio._

import java.net.InetSocketAddress

import cityrover.util.{AppConfig, CassandraConfig}


/** Cassandra session management.
  *
  * Two layers:
  *   - `bootstrap` — no keyspace; used only to run the DDL. Short-lived.
  *   - `runtime`   — with keyspace; used by `CassandraSink` for the
  *                   lifetime of the job. Scoped, closed on shutdown.
  *
  * The driver's `CqlSession` is thread-safe and should be created once
  * per JVM (or once per sink writer subtask), not per request. Both
  * layers reflect that: `bootstrap` is short-lived, `runtime` lives as
  * long as the scope that provides it.
  */
object CassandraSession:

  /** Short-lived session used to create the keyspace and table.
    *
    * Opened once at startup. The caller is expected to run
    * `CassandraSchema.initialize` inside the scope, then let the scope
    * close the session. Do NOT hand this session to the sink — it has
    * no keyspace bound, so unqualified table names fail.
    */
  val bootstrap: ZLayer[AppConfig, Throwable, CqlSession] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[AppConfig]
        session <- ZIO.acquireRelease(
          ZIO.attempt(openSession(cfg.cassandra, withKeyspace = false))
        )(s => ZIO.attempt(s.close()).orDie)
      yield session
    }

  /** Long-lived runtime session, bound to the configured keyspace.
    *
    * Provided to `CassandraSink` for its lifetime. Scoped: closes on
    * job shutdown or on failure of the scope.
    */
  val runtime: ZLayer[AppConfig, Throwable, CqlSession] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[AppConfig]
        session <- ZIO.acquireRelease(
          ZIO.attempt(openSession(cfg.cassandra, withKeyspace = true))
        )(s => ZIO.attempt(s.close()).orDie)
      yield session
    }

  /** Opens a CqlSession. Does NOT register a finalizer — the caller is
    * responsible for closing. The `bootstrap`/`runtime` layers above
    * handle that; this is exposed for tests and one-off diagnostics.
    *
    * `withKeyspace = false` for bootstrap (DDL needs to CREATE KEYSPACE,
    * which can't reference an existing keyspace). `true` for runtime.
    */
  private[cassandra] def openSession(
    cfg: CassandraConfig,
    withKeyspace: Boolean
  ): CqlSession =
    val builder = CqlSession.builder()

    cfg.contactPointList.foreach { host =>
      builder.addContactPoint(new InetSocketAddress(host, cfg.port))
    }

    builder.withLocalDatacenter(cfg.localDatacenter)

    if withKeyspace then builder.withKeyspace(cfg.keyspace)

    if cfg.hasAuth then
      builder.withAuthCredentials(cfg.username, cfg.password)

    builder.build()
