package streamio.cassandra

import com.datastax.oss.driver.api.core.CqlSession

import java.net.InetSocketAddress
import java.util


/**
  * CassandraSessionFactory
  *
  * Responsible only for creating CqlSession instances from connector options.
  *
  * Used by:
  *   - Table API sink (CassandraDynamicTableSink / CassandraTableSinkWriter)
  *   - DataStream API sink (CassandraSinkWriter)
  *
  * Options expected (from Python DDL):
  *   - hosts            : comma-separated list
  *   - port             : optional, default 9042
  *   - keyspace         : optional
  *   - local_datacenter : optional, default "datacenter1"
  *   - username         : optional
  *   - password         : optional
  */
object CassandraSessionFactory:

  /** Create a CqlSession from explicit parameters */
  def createSession(
    hosts: List[String],
    port: Int,
    keyspace: Option[String],
    localDatacenter: String,
    username: Option[String] = None,
    password: Option[String] = None
  ): CqlSession =
    val builder = CqlSession.builder()

    hosts.foreach { host =>
      builder.addContactPoint(InetSocketAddress(host, port))
    }

    builder.withLocalDatacenter(localDatacenter)

    keyspace.foreach(builder.withKeyspace)

    (username, password) match
      case (Some(u), Some(p)) => builder.withAuthCredentials(u, p)
      case _ => ()

    builder.build()

  /** Create a CqlSession from Flink connector options */
  def fromOptions(options: util.Map[String, String]): CqlSession =
    val hostsStr        = options.getOrDefault("hosts", "localhost")
    val hosts           = hostsStr.split(",").map(_.trim).filter(_.nonEmpty).toList

    val portStr         = options.getOrDefault("port", "9042")
    val port            = portStr.toInt

    val keyspaceOpt     = Option(options.get("keyspace")).filter(_.nonEmpty)

    val dc              = options.getOrDefault("local_datacenter", "datacenter1")

    val usernameOpt     = Option(options.get("username")).filter(_.nonEmpty)
    val passwordOpt     = Option(options.get("password")).filter(_.nonEmpty)

    createSession(
      hosts           = hosts,
      port            = port,
      keyspace        = keyspaceOpt,
      localDatacenter = dc,
      username        = usernameOpt,
      password        = passwordOpt
    )

end CassandraSessionFactory
