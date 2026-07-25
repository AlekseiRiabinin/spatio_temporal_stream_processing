package cityrover.spark.trajectory

import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import org.apache.spark.sql.streaming.Trigger
import com.typesafe.config.ConfigFactory


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

    import spark.implicits._

    // Kafka → telemetry stream
    val telemetryStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .load()
      .select(F.from_json(F.col("value").cast("string"), TelemetrySchema.schema).as("json"))
      .select("json.*")

    // Load graph-engine output (future use: snapping trajectories to edges)
    val nodes = spark.read.parquet(s"$graphPath/nodes.parquet")
    val edges = spark.read.parquet(s"$graphPath/edges.parquet")

    // Build trajectories per rover
    val trajectories = TrajectoryBuilder.build(telemetryStream)

    // Write GeoJSON + Parquet
    val query = trajectories.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {

          // Write Parquet batch
          batchDF.write.mode("overwrite")
            .parquet(s"$outputDir/parquet/batch_$batchId")

          // Write GeoJSON per rover
          batchDF.collect().foreach { row =>
            val roverId = row.getAs[String]("roverId")
            val coords  = row.getAs[Seq[Seq[Double]]]("coords")

            val geojson = GeoJsonWriter.toLineString(
              roverId,
              coords,
              Map("batchId" -> io.circe.Json.fromLong(batchId))
            )

            GeoJsonWriter.writeToFile(
              s"$outputDir/geojson/$roverId.json",
              geojson
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
