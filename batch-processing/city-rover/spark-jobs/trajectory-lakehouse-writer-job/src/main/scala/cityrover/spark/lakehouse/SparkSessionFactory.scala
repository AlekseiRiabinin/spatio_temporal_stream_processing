package cityrover.spark.lakehouse

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession


object SparkSessionFactory {

  def create(config: Config): SparkSession = {

    // ------------------------------------------------------------
    // 1. Read configuration
    // ------------------------------------------------------------

    val catalogName = config.getString("cityrover.iceberg.catalog")
    val warehouse = config.getString("cityrover.iceberg.warehouse")
    val endpoint = config.getString("cityrover.iceberg.s3.endpoint")
    val accessKey = config.getString("cityrover.iceberg.s3.access-key-id")
    val secretKey = config.getString("cityrover.iceberg.s3.secret-access-key")
    val pathStyleAccess = config.getBoolean("cityrover.iceberg.s3.path-style-access")

    // ------------------------------------------------------------
    // 2. Build SparkSession
    //
    // Iceberg configuration is applied BEFORE the SparkSession is created.
    // ------------------------------------------------------------

    SparkSession
      .builder()
      .appName("cityrover-trajectory-lakehouse-writer-job")

      // ----------------------------------------------------------
      // Iceberg Spark extensions
      // ----------------------------------------------------------

      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )

      // ----------------------------------------------------------
      // Iceberg catalog
      // ----------------------------------------------------------

      .config(
        s"spark.sql.catalog.$catalogName",
        "org.apache.iceberg.spark.SparkCatalog"
      )

      .config(
        s"spark.sql.catalog.$catalogName.type",
        "hadoop"
      )

      .config(
        s"spark.sql.catalog.$catalogName.warehouse",
        warehouse
      )

      // ----------------------------------------------------------
      // Iceberg S3FileIO
      // ----------------------------------------------------------

      .config(
        s"spark.sql.catalog.$catalogName.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
      )

      .config(
        s"spark.sql.catalog.$catalogName.s3.endpoint",
        endpoint
      )

      .config(
        s"spark.sql.catalog.$catalogName.s3.access-key-id",
        accessKey
      )

      .config(
        s"spark.sql.catalog.$catalogName.s3.secret-access-key",
        secretKey
      )

      .config(
        s"spark.sql.catalog.$catalogName.s3.path-style-access",
        pathStyleAccess.toString
      )

      // ----------------------------------------------------------
      // Create SparkSession
      // ----------------------------------------------------------

      .getOrCreate()
  }
}
