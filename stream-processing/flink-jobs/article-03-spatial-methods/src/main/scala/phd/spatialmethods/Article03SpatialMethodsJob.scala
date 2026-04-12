package phd.spatialmethods

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import phd.spatialmethods.model.GeoEvent
import phd.spatialmethods.model.Interaction
import phd.spatialmethods.pipeline.SpatialStreamPipeline


/**
 * Article03SpatialMethodsJob
 *
 * Entry point for Flink job implementing spatio-temporal processing of GeoEvents.
 */
object Article03SpatialMethodsJob {

  // JSON mapper
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {

    println("[MAIN] action=start job=Article03SpatialMethodsJob")

    // ------------------------------------------------------------------
    // 1. Flink environment (Scala API)
    // ------------------------------------------------------------------
    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    println(
      s"[MAIN] action=env parallelism=${env.getParallelism}"
    )

    // ------------------------------------------------------------------
    // 2. Kafka configuration
    // ------------------------------------------------------------------
    val bootstrap =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092")

    val topic =
      sys.env.getOrElse("KAFKA_TOPIC", "spatial-events")

    println(
      s"[MAIN] action=kafkaConfig bootstrap=$bootstrap topic=$topic"
    )

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", bootstrap)
    kafkaProps.setProperty("group.id", "article03-spatial-methods")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      topic,
      new SimpleStringSchema(),
      kafkaProps
    )

    // ------------------------------------------------------------------
    // 3. Deserialize JSON -> GeoEvent
    // ------------------------------------------------------------------
    println("[MAIN] action=deserialization status=starting")

    implicit val geoEventTypeInfo: TypeInformation[GeoEvent] =
      createTypeInformation[GeoEvent]

    val geoEventStream: DataStream[GeoEvent] =
      env.addSource(kafkaConsumer)
        .map(json => mapper.readValue(json, classOf[GeoEvent]))
        .filter(_.isValid)

    println("[MAIN] action=deserialization status=ready")

    // ------------------------------------------------------------------
    // 4. Apply spatial-temporal processing pipeline
    // ------------------------------------------------------------------
    println("[MAIN] action=pipelineInit status=starting")

    val interactionsStream: DataStream[Interaction] =
      SpatialStreamPipeline.buildPipeline(env, geoEventStream)

    println("[MAIN] action=pipelineInit status=ready")

    // ------------------------------------------------------------------
    // 5. Output results
    // ------------------------------------------------------------------
    println("[MAIN] action=output status=printing")

    var throughputCounter = 0L
    var lastReportTime = System.currentTimeMillis()

    interactionsStream
      .map { interaction =>

        // Throughput counter
        throughputCounter += 1
        val now = System.currentTimeMillis()

        // Emit throughput once per second
        if (now - lastReportTime >= 1000) {
          println(
            s"[THROUGHPUT] interactionsPerSec=$throughputCounter timestamp=$now"
          )
          throughputCounter = 0
          lastReportTime = now
        }

        mapper.writeValueAsString(interaction)
      }
      .print()

    // ------------------------------------------------------------------
    // 6. Execute job
    // ------------------------------------------------------------------
    println("[MAIN] action=execute job=Article03SpatialMethodsJob")

    env.execute("Article 03: Spatio-Temporal Data Stream Processing")
  }
}
