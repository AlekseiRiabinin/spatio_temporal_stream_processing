package cityrover.spark.lakehouse

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}


object KafkaSource {

  // ----------------------------------------------------------------
  // Read a Kafka topic as a streaming DataFrame
  // ----------------------------------------------------------------

  def readTopic(
    spark: SparkSession,
    config: Config,
    topic: String,
    startingOffsets: String = "latest"
  ): DataFrame = {

    val bootstrapServers = config.getString("cityrover.kafka.bootstrap")

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) AS json")
  }

  // ----------------------------------------------------------------
  // Raw rover telemetry
  // ----------------------------------------------------------------

  def rawTelemetry(spark: SparkSession, config: Config): DataFrame = {
    val topic = config.getString("cityrover.kafka.topics.raw-telemetry")
    readTopic(spark, config, topic)
  }

  // ----------------------------------------------------------------
  // Enriched telemetry
  // ----------------------------------------------------------------

  def enrichedTelemetry(spark: SparkSession, config: Config): DataFrame = {
    val topic = config.getString("cityrover.kafka.topics.enriched-telemetry")
    readTopic(spark, config, topic)
  }

  // ----------------------------------------------------------------
  // Analytics stream
  // ----------------------------------------------------------------

  def analyticsStream(spark: SparkSession, config: Config): DataFrame = {
    val topic = config.getString("cityrover.kafka.topics.analytics")
    readTopic(spark, config, topic)
  }
}
