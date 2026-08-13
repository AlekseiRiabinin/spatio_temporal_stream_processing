package cityrover.spark.trajectory

import org.apache.spark.sql.{SparkSession, DataFrame, Row, functions => F}
import org.apache.spark.sql.streaming.Trigger
import com.typesafe.config.ConfigFactory
import io.circe.Json


object TrajectoryVisualizerMain {

  def main(args: Array[String]): Unit = {

    // Load configuration
    val config = ConfigFactory.load()

    val kafkaBootstrap = config.getString("telemetry.kafka.bootstrap")
    val kafkaTopic     = config.getString("telemetry.kafka.topic")
    val graphPath      = config.getString("graph.outputDir")
    val outputDir      = config.getString("visualizer.outputDir")

    // Spark session
    val spark = SparkSession.builder()
      .appName("cityrover-trajectory-visualizer")
      .getOrCreate()

    // Kafka → telemetry stream
    val telemetryStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .load()
      .select(
        F.from_json(
          F.col("value").cast("string"),
          TelemetrySchema.schema
        ).as("json")
      )
      .select("json.*")

    // Load graph-engine output.
    // Currently loaded for future trajectory/edge processing.
    val nodes = spark.read.parquet(s"$graphPath/nodes.parquet")
    val edges = spark.read.parquet(s"$graphPath/edges.parquet")

    // Prevent unused-value warnings in some build configurations.
    nodes.schema
    edges.schema

    // Build trajectories per rover.
    //
    // Result:
    //   roverId
    //   coords
    //   positions
    val trajectories = TrajectoryBuilder.build(telemetryStream)

    // Write GeoJSON + replay JSON + Parquet
    val query = trajectories.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        if (!batchDF.isEmpty) {

          // ------------------------------------------------------------
          // 1. Write Parquet batch
          // ------------------------------------------------------------
          batchDF.write
            .mode("overwrite")
            .parquet(s"$outputDir/parquet/batch_$batchId")

          // ------------------------------------------------------------
          // 2. Write trajectory and replay data for each rover
          // ------------------------------------------------------------
          batchDF.collect().foreach { row =>

            val roverId = row.getAs[String]("roverId")

            // ----------------------------------------------------------
            // LineString coordinates
            // ----------------------------------------------------------
            val coords =
              row.getAs[Seq[Seq[Double]]]("coords")

            val trajectoryGeoJson =
              GeoJsonWriter.toLineString(
                roverId,
                coords,
                Map("batchId" -> Json.fromLong(batchId))
              )

            GeoJsonWriter.writeToFile(
              s"$outputDir/geojson/$roverId.json",
              trajectoryGeoJson
            )

            // ----------------------------------------------------------
            // Timestamped positions for offline replay
            // ----------------------------------------------------------
            val positions =
              row
                .getAs[Seq[Row]]("positions")
                .map { position =>
                  val ts = position.getAs[Long]("ts")
                  val lat = position.getAs[Double]("lat")
                  val lon = position.getAs[Double]("lon")
                  val speed = position.getAs[Double]("speed")
                  val heading = position.getAs[Double]("heading")
                  val edgeId = Option(position.getAs[String]("edgeId"))
                  val routeId = Option(position.getAs[String]("routeId"))

                  Map(
                    "ts" -> Json.fromLong(ts),
                    "lat" -> Json.fromDoubleOrNull(lat),
                    "lon" -> Json.fromDoubleOrNull(lon),
                    "speed" -> Json.fromDoubleOrNull(speed),
                    "heading" -> Json.fromDoubleOrNull(heading),
                    "edgeId" -> edgeId
                      .map(Json.fromString)
                      .getOrElse(Json.Null),
                    "routeId" -> routeId
                      .map(Json.fromString)
                      .getOrElse(Json.Null)
                  )
                }

            val replayGeoJson =
              GeoJsonWriter.toPositionFeatureCollection(
                roverId,
                positions,
                Map("batchId" -> Json.fromLong(batchId))
              )

            GeoJsonWriter.writeToFile(
              s"$outputDir/replay/$roverId.json",
              replayGeoJson
            )
          }
        }
      }
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("update")
      .start()

    query.awaitTermination()
  }
}
