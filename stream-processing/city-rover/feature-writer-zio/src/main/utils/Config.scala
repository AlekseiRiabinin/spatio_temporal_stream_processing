package cityrover.util

import zio._
import zio.config._
import zio.config.magnolia._
import zio.config.typesafe.TypesafeConfigProvider

import java.io.File


// =============================================================================
// Typed configuration tree, mirroring src/main/resources/application.conf
// =============================================================================

final case class KafkaConsumerConfig(
  offsetReset: String,
  fetchMinBytes: Int,
  fetchMaxWaitMs: Int,
  /** Kafka consumer isolation level. `read_uncommitted` matches the
    * at-least-once sink on the transform side; `read_committed` would
    * gate visibility on checkpoint completion. */
  isolationLevel: String,
  /** Max parallelism for partition-level streams. Used only if the
    * pipeline uses `partitionedStream`; `plainStream` ignores it.
    * Kept for future throughput tuning. */
  partitionStreamParallelism: Int
)

final case class KafkaConfig(
  bootstrapServers: String,
  enrichedTopic: String,
  groupId: String,
  consumer: KafkaConsumerConfig
)

final case class CassandraDriverConfig(
  requestTimeoutSeconds: Int,
  maxRequestsPerConnection: Int,
  consistency: String
)

final case class CassandraConfig(
  contactPoints: String,
  port: Int,
  localDatacenter: String,
  keyspace: String,
  table: String,
  username: String,
  password: String,
  driver: CassandraDriverConfig
) {
  /** Contact points as a `List[String]`, split on comma. */
  def contactPointList: List[String] =
    contactPoints.split(",").map(_.trim).filter(_.nonEmpty).toList

  def hasAuth: Boolean = username.nonEmpty
}

final case class MetricsRegisterConfig(
  kafkaToFlink: Boolean,
  queue: Boolean,
  transform: Boolean,
  emit: Boolean,
  endToEnd: Boolean
)

final case class MetricsConfig(
  enabled: Boolean,
  namespace: String,
  subgroup: String,
  httpPort: Int,
  histogramBucketsMs: List[Double],
  register: MetricsRegisterConfig
) {
  def buckets: Chunk[Double] = Chunk.fromIterable(histogramBucketsMs)
}

final case class AppAppConfig(
  logLevel: String,
  shutdownDrainTimeoutSeconds: Int
) {
  def shutdownDrainTimeout: Duration = shutdownDrainTimeoutSeconds.seconds
}

final case class AppConfig(
  kafka: KafkaConfig,
  cassandra: CassandraConfig,
  metrics: MetricsConfig,
  app: AppAppConfig
)

// =============================================================================
// ConfigLoader
// =============================================================================

object ConfigLoader {

  /** Descriptor derived by Magnolia from AppConfig, nested under "cityrover"
    * to match application.conf's top-level block.
    */
  private val appConfigDescriptor: Config[AppConfig] =
    deriveConfig[AppConfig].nested("cityrover")

  /** Load from application.conf on the classpath. */
  val live: ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(
      read(appConfigDescriptor from TypesafeConfigProvider.fromResourcePath())
    )

  /** For tests: load from an explicit HOCON string. */
  def fromHoconString(hocon: String): ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(
      read(appConfigDescriptor from TypesafeConfigProvider.fromHoconString(hocon))
    )

  /** For tests: load from an explicit file. */
  def fromFile(file: File): ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(
      read(appConfigDescriptor from TypesafeConfigProvider.fromHoconFile(file))
    )

  // ---------------------------------------------------------------------------
  // Convenience accessors
  // ---------------------------------------------------------------------------

  val kafka: URIO[AppConfig, KafkaConfig] =
    ZIO.service[AppConfig].map(_.kafka)

  val consumer: URIO[AppConfig, KafkaConsumerConfig] =
    ZIO.service[AppConfig].map(_.kafka.consumer)

  val cassandra: URIO[AppConfig, CassandraConfig] =
    ZIO.service[AppConfig].map(_.cassandra)

  val cassandraDriver: URIO[AppConfig, CassandraDriverConfig] =
    ZIO.service[AppConfig].map(_.cassandra.driver)

  val metrics: URIO[AppConfig, MetricsConfig] =
    ZIO.service[AppConfig].map(_.metrics)

  val app: URIO[AppConfig, AppAppConfig] =
    ZIO.service[AppConfig].map(_.app)
}
