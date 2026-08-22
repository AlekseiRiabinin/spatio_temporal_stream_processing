package cityrover.spark.lakehouse

import org.apache.spark.sql.types.{
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}


object TelemetrySchemas {

  // ----------------------------------------------------------------
  // Raw telemetry schema
  //
  // This schema represents the JSON payload produced by the
  // CityRover telemetry producer.
  // ----------------------------------------------------------------

  val rawSchema: StructType =
    StructType(
      Seq(
        StructField(name = "roverId", dataType = StringType, nullable = false),
        StructField(name = "ts", dataType = LongType, nullable = false),
        StructField(name = "lat", dataType = DoubleType, nullable = true),
        StructField(name = "lon", dataType = DoubleType, nullable = true),
        StructField(name = "speed", dataType = DoubleType, nullable = true),
        StructField(name = "heading", dataType = DoubleType, nullable = true),
        StructField(name = "edgeId", dataType = StringType, nullable = true),
        StructField(name = "routeId", dataType = StringType, nullable = true)
      )
    )
}
