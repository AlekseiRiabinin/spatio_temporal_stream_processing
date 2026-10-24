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
  partitionStreamParallelism: Int
)

final case class KafkaProducerConfig(
  acks: String,
  lingerMs: Int,
  compressionType: String,
  retries: Int,
  maxInFlightRequestsPerConnection: Int
)

final case class KafkaConfig(
  bootstrapServers: String,
  rawTopic: String,
  enrichedTopic: String,
  groupId: String,
  consumer: KafkaConsumerConfig,
  producer: KafkaProducerConfig
)

final case class WindowConfig(
  size5sSeconds: Int,
  size30sSeconds: Int,
  size1mSeconds: Int,
  size5mSeconds: Int,
  allowedLatenessMs: Long,
  idlenessMs: Long,
  maxPendingWindowsPerRover: Int
) {
  def size5s: Duration  = size5sSeconds.seconds
  def size30s: Duration = size30sSeconds.seconds
  def size1m: Duration  = size1mSeconds.seconds
  def size5m: Duration  = size5mSeconds.seconds

  def allowedLateness: Duration = allowedLatenessMs.millis
  def idleness: Duration        = idlenessMs.millis
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
  window: WindowConfig,
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

  /** Load from application.conf on the classpath.
    *
    * Uses TypesafeConfigProvider.fromResourcePath() and the
    * `read(descriptor from provider)` pattern — the correct API for
    * zio-config 4.x with zio-config-typesafe.
    */
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

  val kafka: URIO[AppConfig, KafkaConfig]     = ZIO.service[AppConfig].map(_.kafka)
  val window: URIO[AppConfig, WindowConfig]   = ZIO.service[AppConfig].map(_.window)
  val metrics: URIO[AppConfig, MetricsConfig] = ZIO.service[AppConfig].map(_.metrics)
  val app: URIO[AppConfig, AppAppConfig]      = ZIO.service[AppConfig].map(_.app)
}
