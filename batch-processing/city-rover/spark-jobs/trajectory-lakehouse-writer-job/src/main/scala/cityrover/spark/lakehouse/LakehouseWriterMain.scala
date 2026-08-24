package cityrover.spark.lakehouse

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession


object LakehouseWriterMain {

  def main(args: Array[String]): Unit = {

    // ------------------------------------------------------------
    // 1. Load application configuration
    // ------------------------------------------------------------

    val config: Config = ConfigFactory.load()

    // ------------------------------------------------------------
    // 2. Validate required configuration
    // ------------------------------------------------------------

    validateConfig(config)

    // ------------------------------------------------------------
    // 3. Create SparkSession
    // ------------------------------------------------------------

    val spark: SparkSession = SparkSessionFactory.create(config)

    try {

      println("==============================================")
      println(" CityRover Lakehouse Writer")
      println("==============================================")

      println(
        s"Kafka bootstrap: ${config.getString("cityrover.kafka.bootstrap")}"
      )

      println(
        s"Iceberg catalog: ${config.getString("cityrover.iceberg.catalog")}"
      )

      println(
        s"Iceberg metastore URI: ${config.getString("cityrover.iceberg.metastore-uri")}"
      )

      // ----------------------------------------------------------
      // 4. Read telemetry from Kafka
      // ----------------------------------------------------------

      val rawTelemetryDF = KafkaSource.rawTelemetry(spark, config)

      // ----------------------------------------------------------
      // 5. Parse and transform telemetry
      // ----------------------------------------------------------

      val telemetryDF = TelemetryTransformer.transform(rawTelemetryDF)

      // ----------------------------------------------------------
      // 6. Write telemetry to Iceberg
      // ----------------------------------------------------------

      val query = IcebergTableWriter.write(telemetryDF, config)

      // ----------------------------------------------------------
      // 7. Wait for the streaming query
      // ----------------------------------------------------------

      query.awaitTermination()

    } catch {

      case exception: Throwable =>
        System.err.println("CityRover Lakehouse Writer failed.")
        exception.printStackTrace()
        throw exception

    } finally {
      spark.stop()
    }
  }

  // ----------------------------------------------------------------
  // Configuration validation (HiveCatalog version)
  // ----------------------------------------------------------------

  private def validateConfig(config: Config): Unit = {

    val requiredPaths = Seq(
      "cityrover.kafka.bootstrap",
      "cityrover.kafka.topics.raw-telemetry",
      "cityrover.iceberg.catalog",
      "cityrover.iceberg.metastore-uri",
      "cityrover.iceberg.namespace",
      "cityrover.iceberg.table",
      "cityrover.iceberg.checkpoint",
      "cityrover.iceberg.s3.endpoint",
      "cityrover.iceberg.s3.access-key-id",
      "cityrover.iceberg.s3.secret-access-key",
      "cityrover.iceberg.s3.path-style-access"
    )

    requiredPaths.foreach { path =>
      require(
        config.hasPath(path),
        s"Missing required configuration: $path"
      )
    }
  }
}
