package cityrover.spark.lakehouse

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}


object IcebergTableWriter {

  // ----------------------------------------------------------------
  // Write telemetry DataFrame to Iceberg using Structured Streaming
  // ----------------------------------------------------------------

  def write(telemetry: DataFrame, config: Config): StreamingQuery = {

    val catalog = config.getString("cityrover.iceberg.catalog")
    val namespace = config.getString("cityrover.iceberg.namespace")
    val table = config.getString("cityrover.iceberg.table")
    val checkpointLocation = config.getString("cityrover.iceberg.checkpoint")

    // --------------------------------------------------------------
    // Fully-qualified Iceberg identifier:
    //
    // catalog.namespace.table
    //
    // Example:
    //
    // cityrover.cityrover.telemetry_raw
    // --------------------------------------------------------------

    val tableIdentifier = s"$catalog.$namespace.$table"

    println(
      s"Iceberg target table: $tableIdentifier"
    )

    println(
      s"Checkpoint location: $checkpointLocation"
    )

    telemetry.writeStream
      .format("iceberg")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .toTable(tableIdentifier)
  }
}
