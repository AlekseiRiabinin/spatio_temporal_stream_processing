package streamio.cassandra.table

import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.factories.{
  DynamicTableFactory,
  DynamicTableSinkFactory,
  FactoryUtil
}

import java.util


/**
 * CassandraTableFactory
 *
 * Flink SPI entry point for the Cassandra connector.
 *
 * Responsible for:
 *   - declaring connector identifier ("cassandra")
 *   - defining required + optional options
 *   - validating options
 *   - constructing CassandraDynamicTableSink
 */
class CassandraTableFactory extends DynamicTableSinkFactory:

  // ---------------------------------------------------------------------------
  // Connector identifier
  // ---------------------------------------------------------------------------

  override def factoryIdentifier(): String =
    "cassandra"

  // ---------------------------------------------------------------------------
  // Connector options
  // ---------------------------------------------------------------------------

  val HOSTS: ConfigOption[String] =
    ConfigOptions
      .key("hosts")
      .stringType()
      .noDefaultValue()

  val TABLE: ConfigOption[String] =
    ConfigOptions
      .key("table")
      .stringType()
      .noDefaultValue()

  val KEYSPACE: ConfigOption[String] =
    ConfigOptions
      .key("keyspace")
      .stringType()
      .defaultValue("cityrover")

  val PORT: ConfigOption[Integer] =
    ConfigOptions
      .key("port")
      .intType()
      .defaultValue(9042)

  val LOCAL_DATACENTER: ConfigOption[String] =
    ConfigOptions
      .key("local_datacenter")
      .stringType()
      .defaultValue("datacenter1")

  val USERNAME: ConfigOption[String] =
    ConfigOptions
      .key("username")
      .stringType()
      .noDefaultValue()

  val PASSWORD: ConfigOption[String] =
    ConfigOptions
      .key("password")
      .stringType()
      .noDefaultValue()

  val CONSISTENCY: ConfigOption[String] =
    ConfigOptions
      .key("consistency")
      .stringType()
      .defaultValue("LOCAL_QUORUM")

  val TTL_SECONDS: ConfigOption[Integer] =
    ConfigOptions
      .key("ttl_seconds")
      .intType()
      .defaultValue(0)

  val BATCH_SIZE: ConfigOption[Integer] =
    ConfigOptions
      .key("batch_size")
      .intType()
      .defaultValue(1)

  val MAX_INFLIGHT: ConfigOption[Integer] =
    ConfigOptions
      .key("max_inflight")
      .intType()
      .defaultValue(100)

  // ---------------------------------------------------------------------------
  // Required options
  // ---------------------------------------------------------------------------

  override def requiredOptions(): util.Set[ConfigOption[?]] =
    util.Set.of(
      HOSTS,
      TABLE
    )

  // ---------------------------------------------------------------------------
  // Optional options
  // ---------------------------------------------------------------------------

  override def optionalOptions(): util.Set[ConfigOption[?]] =
    util.Set.of(
      KEYSPACE,
      PORT,
      LOCAL_DATACENTER,
      USERNAME,
      PASSWORD,
      CONSISTENCY,
      TTL_SECONDS,
      BATCH_SIZE,
      MAX_INFLIGHT
    )

  // ---------------------------------------------------------------------------
  // Create sink
  // ---------------------------------------------------------------------------

  override def createDynamicTableSink(
      context: DynamicTableFactory.Context
  ): DynamicTableSink =

    val helper =
      FactoryUtil.createTableFactoryHelper(this, context)

    helper.validate()

    val options =
      context.getCatalogTable.getOptions

    new CassandraTableSink(
      context.getCatalogTable.getResolvedSchema,
      options
    )

end CassandraTableFactory
