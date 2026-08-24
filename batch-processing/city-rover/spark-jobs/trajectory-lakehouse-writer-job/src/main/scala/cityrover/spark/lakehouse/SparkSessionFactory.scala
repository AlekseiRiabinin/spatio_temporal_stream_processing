package cityrover.spark.lakehouse

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession


object SparkSessionFactory {

  def create(config: Config): SparkSession = {

    // ============================================================
    // 1. Read Iceberg configuration
    // ============================================================

    val catalogName =
      config.getString("cityrover.iceberg.catalog")

    val metastoreUri =
      config.getString("cityrover.iceberg.metastore-uri")

    val endpoint =
      config.getString("cityrover.iceberg.s3.endpoint")

    val accessKey =
      config.getString("cityrover.iceberg.s3.access-key-id")

    val secretKey =
      config.getString("cityrover.iceberg.s3.secret-access-key")

    val pathStyleAccess =
      config.getBoolean(
        "cityrover.iceberg.s3.path-style-access"
      )

    // ============================================================
    // 2. Create SparkSession
    // ============================================================

    SparkSession
      .builder()
      .appName("cityrover-trajectory-lakehouse-writer-job")

      // ==========================================================
      // Iceberg Spark extensions
      // ==========================================================

      .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      )

      // ==========================================================
      // Iceberg HiveCatalog
      // ==========================================================

      .config(
        s"spark.sql.catalog.$catalogName",
        "org.apache.iceberg.spark.SparkCatalog"
      )
      .config(
        s"spark.sql.catalog.$catalogName.type",
        "hive"
      )
      .config(
        s"spark.sql.catalog.$catalogName.uri",
        metastoreUri
      )

      // ==========================================================
      // Iceberg S3FileIO (MinIO)
      // ==========================================================

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

      // ==========================================================
      // Hadoop S3A configuration (required for Spark)
      // ==========================================================

      .config("spark.hadoop.fs.s3a.endpoint", endpoint)
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", pathStyleAccess.toString)
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      // ==========================================================
      // Create session
      // ==========================================================

      .getOrCreate()
  }
}
