package cityrover.spark.lakehouse

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}


object IcebergTableWriter {

  def write(telemetry: DataFrame, config: Config): StreamingQuery = {

    val catalog            = config.getString("cityrover.iceberg.catalog")
    val namespace          = config.getString("cityrover.iceberg.namespace")
    val table              = config.getString("cityrover.iceberg.table")
    val checkpointLocation = config.getString("cityrover.iceberg.checkpoint")
    val tableLocation      = config.getString("cityrover.iceberg.table-location")

    val tableIdentifier = s"$catalog.$namespace.$table"

    println(s"Iceberg target table: $tableIdentifier")
    println(s"Checkpoint location: $checkpointLocation")
    println(s"Iceberg table location: $tableLocation")

    telemetry.sparkSession.sql(
      s"CREATE NAMESPACE IF NOT EXISTS $catalog.$namespace"
    )

    telemetry.sparkSession.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $tableIdentifier (
         |  rover_id STRING,
         |  ts BIGINT,
         |  lat DOUBLE,
         |  lon DOUBLE,
         |  speed DOUBLE,
         |  heading DOUBLE,
         |  edge_id STRING,
         |  route_id STRING,
         |  event_time TIMESTAMP,
         |  event_date DATE,
         |  speed_kmh DOUBLE
         |)
         |USING ICEBERG
         |LOCATION '$tableLocation'
         |""".stripMargin)

    telemetry.writeStream
      .format("iceberg")
      .option("path", tableIdentifier)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
  }
}
