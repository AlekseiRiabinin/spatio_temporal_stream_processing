package phd.architecture.stream

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import phd.architecture.model.Event
import phd.architecture.model.TypeInfos._
import phd.architecture.util.{GeometryUtils, TimeUtils}
import phd.architecture.metrics.Metrics

import java.io.IOException

object KafkaSourceFactory {

  private val objectMapper = new ObjectMapper()

  /**
   * Creates a Kafka source producing spatial-temporal events.
   */
  def createSpatialEventSource(env: StreamExecutionEnvironment): DataStream[Event] = {

    val source =
      KafkaSource.builder[Event]()
        .setBootstrapServers("kafka-1:19092,kafka-2:19094")
        .setTopics("spatial-events")
        .setGroupId("article-01-architecture")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new EventDeserializationSchema)
        .build()

    env.fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "KafkaSource(spatial-events)"
    )
  }

  /**
   * Custom JSON → Event deserializer with metrics.
   */
  private class EventDeserializationSchema
      extends DeserializationSchema[Event]
      with Serializable {

    @throws[IOException]
    override def deserialize(message: Array[Byte]): Event = {
      val jsonStr = new String(message, "UTF-8")
      val jsonNode: JsonNode = objectMapper.readTree(jsonStr)

      // Extract fields safely
      val id = Option(jsonNode.get("id"))
        .map(_.asText())
        .getOrElse(throw new IllegalArgumentException(s"'id' field missing in JSON: $jsonStr"))

      val wkt = Option(jsonNode.get("wkt"))
        .map(_.asText())
        .getOrElse(throw new IllegalArgumentException(s"'wkt' field missing in JSON: $jsonStr"))

      val producerTs = Option(jsonNode.get("timestamp"))
        .map(_.asLong())
        .getOrElse(throw new IllegalArgumentException(s"'timestamp' field missing in JSON: $jsonStr"))

      // Metrics
      Metrics.eventsConsumed.inc()

      val now = System.currentTimeMillis()
      val latencySeconds = (now - producerTs) / 1000.0
      Metrics.ingestionLatency.observe(latencySeconds)

      Event(
        id = id,
        geometry = GeometryUtils.fromWKT(wkt),
        eventTime = TimeUtils.toMillis(producerTs),
        attributes = Map.empty
      )
    }

    override def isEndOfStream(nextElement: Event): Boolean = false

    override def getProducedType: TypeInformation[Event] =
      TypeInformation.of(classOf[Event])
  }
}
