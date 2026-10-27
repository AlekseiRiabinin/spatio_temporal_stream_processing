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
  /** Max parallelism for partition-level streams. Used only if the
    * pipeline uses `partitionedStream`; `plainStream` ignores it.
    * Kept for future throughput tuning.
    */
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

  /** Window sizes in milliseconds, authoritative for the window state
    * machine (`WindowTypeTag.windowEndFor`). */
  def sizeMs5s: Long  = size5sSeconds * 1000L
  def sizeMs30s: Long = size30sSeconds * 1000L
  def sizeMs1m: Long  = size1mSeconds * 1000L
  def sizeMs5m: Long  = size5mSeconds * 1000L

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

  private val appConfigDescriptor: Config[AppConfig] =
    deriveConfig[AppConfig].nested("cityrover")

  val live: ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(
      read(appConfigDescriptor from TypesafeConfigProvider.fromResourcePath())
    )

  def fromHoconString(hocon: String): ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(
      read(appConfigDescriptor from TypesafeConfigProvider.fromHoconString(hocon))
    )

  def fromFile(file: File): ZLayer[Any, Config.Error, AppConfig] =
    ZLayer.fromZIO(
      read(appConfigDescriptor from TypesafeConfigProvider.fromHoconFile(file))
    )

  // ---------------------------------------------------------------------------
  // Convenience accessors
  // ---------------------------------------------------------------------------

  val kafka: URIO[AppConfig, KafkaConfig]            = ZIO.service[AppConfig].map(_.kafka)
  val producer: URIO[AppConfig, KafkaProducerConfig] = ZIO.service[AppConfig].map(_.kafka.producer)
  val consumer: URIO[AppConfig, KafkaConsumerConfig] = ZIO.service[AppConfig].map(_.kafka.consumer)
  val window: URIO[AppConfig, WindowConfig]          = ZIO.service[AppConfig].map(_.window)
  val metrics: URIO[AppConfig, MetricsConfig]        = ZIO.service[AppConfig].map(_.metrics)
  val app: URIO[AppConfig, AppAppConfig]             = ZIO.service[AppConfig].map(_.app)
}
