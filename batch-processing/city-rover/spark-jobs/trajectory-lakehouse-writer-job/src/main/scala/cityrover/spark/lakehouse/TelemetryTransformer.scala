package cityrover.spark.lakehouse

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


object TelemetryTransformer {

  // ----------------------------------------------------------------
  // Transform raw Kafka telemetry JSON into a structured DataFrame
  // ----------------------------------------------------------------

  def transform(rawTelemetry: DataFrame): DataFrame = {

    rawTelemetry
      .select(from_json(col("json"), TelemetrySchemas.rawSchema).as("telemetry"))
      .select(col("telemetry.*"))
      .withColumn("event_time", timestamp_millis(col("ts")))
      .withColumn("event_date", to_date(col("event_time")))
      .withColumn(
        "speed_kmh",
        when(
          col("speed").isNotNull && col("speed") >= lit(0.0),
          col("speed") * lit(3.6)
        ).otherwise(
          lit(null)
        )
      )
  }
}
