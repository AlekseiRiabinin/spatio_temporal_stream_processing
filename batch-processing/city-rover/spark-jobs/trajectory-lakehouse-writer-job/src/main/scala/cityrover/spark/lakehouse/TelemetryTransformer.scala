package cityrover.spark.lakehouse

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object TelemetryTransformer {

  // ----------------------------------------------------------------
  // Transform raw Kafka telemetry JSON into a structured DataFrame
  // ----------------------------------------------------------------

  def transform(rawTelemetry: DataFrame): DataFrame = {

    rawTelemetry
      // ------------------------------------------------------------
      // 1. Parse JSON using the CityRover telemetry schema
      // ------------------------------------------------------------

      .select(from_json(col("json"), TelemetrySchemas.rawSchema).as("telemetry"))

      // ------------------------------------------------------------
      // 2. Flatten the parsed telemetry struct
      // ------------------------------------------------------------

      .select(col("telemetry.*"))

      // ------------------------------------------------------------
      // 3. Convert epoch milliseconds to Spark timestamp
      // ------------------------------------------------------------

      .withColumn("event_time", timestamp_millis(col("ts")))

      // ------------------------------------------------------------
      // 4. Derive event date
      // ------------------------------------------------------------

      .withColumn("event_date", to_date(col("event_time")))

      // ------------------------------------------------------------
      // 5. Convert speed from m/s to km/h
      // ------------------------------------------------------------

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
