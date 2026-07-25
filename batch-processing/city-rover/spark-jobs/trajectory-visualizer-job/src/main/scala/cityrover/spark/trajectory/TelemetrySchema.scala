package cityrover.spark.trajectory

import org.apache.spark.sql.types._


object TelemetrySchema {

  /** 
    * Schema for rover telemetry events coming from Kafka.
    * Matches rover-simulator JSON output exactly.
    */
  val schema: StructType = new StructType()
    .add("roverId", StringType, nullable = false)
    .add("ts", LongType, nullable = false)
    .add("lat", DoubleType, nullable = false)
    .add("lon", DoubleType, nullable = false)
    .add("speed", DoubleType, nullable = false)
    .add("heading", DoubleType, nullable = false)
    .add("edgeId", StringType, nullable = true)
    .add("routeId", StringType, nullable = true)
}
