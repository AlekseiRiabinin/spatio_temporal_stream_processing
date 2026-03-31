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

    // ------------------------------------------------------------------
    // 1. Flink environment (Scala API)
    // ------------------------------------------------------------------
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    // ------------------------------------------------------------------
    // 2. Kafka configuration
    // ------------------------------------------------------------------
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "spatial-stream-group")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "geo-events-topic",
      new SimpleStringSchema(),
      kafkaProps
    )

    // ------------------------------------------------------------------
    // 3. Deserialize JSON -> GeoEvent
    // ------------------------------------------------------------------
    // Provide implicit TypeInformation for GeoEvent
    implicit val geoEventTypeInfo: TypeInformation[GeoEvent] = createTypeInformation[GeoEvent]

    val geoEventStream: DataStream[GeoEvent] =
      env.addSource(kafkaConsumer)
        .map(json => mapper.readValue(json, classOf[GeoEvent]))
        .filter(_.isValid)

    // ------------------------------------------------------------------
    // 4. Apply spatial-temporal processing pipeline
    // ------------------------------------------------------------------
    val interactionsStream: DataStream[Interaction] =
      SpatialStreamPipeline.buildPipeline(env, geoEventStream)

    // ------------------------------------------------------------------
    // 5. Output results
    // ------------------------------------------------------------------
    interactionsStream
      .map(interaction => mapper.writeValueAsString(interaction))
      .print()

    // ------------------------------------------------------------------
    // 6. Execute job
    // ------------------------------------------------------------------
    env.execute("Article 03: Spatio-Temporal Data Stream Processing")
  }
}
