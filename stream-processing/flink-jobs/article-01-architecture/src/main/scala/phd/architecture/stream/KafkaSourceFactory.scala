package phd.architecture.stream

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.io.IOException

import phd.architecture.model.Event
import phd.architecture.model.TypeInfos._
import phd.architecture.util.{GeometryUtils, TimeUtils}


object KafkaSourceFactory {

  private val objectMapper = new ObjectMapper()

  /**
   * Creates a Kafka source producing spatial-temporal events.
   */
  def createSpatialEventSource(env: StreamExecutionEnvironment): DataStream[Event] = {

    val source =
      KafkaSource.builder[Event]()
        .setBootstrapServers("kafka-1:19092")
        .setTopics("spatial-events")
        .setGroupId("article-01-architecture")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(new EventKafkaRecordDeserializationSchema)
        .build()

    env.fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "KafkaSource(spatial-events)"
    )
  }

  /**
   * Minimal Kafka record deserializer (no metrics — Flink 1.17 cannot expose them here).
   */
  private class EventKafkaRecordDeserializationSchema
      extends KafkaRecordDeserializationSchema[Event]
      with Serializable {

    @throws[IOException]
    override def deserialize(
        record: ConsumerRecord[Array[Byte], Array[Byte]],
        out: Collector[Event]
    ): Unit = {
      if (record == null || record.value() == null) return

      val jsonStr = new String(record.value(), "UTF-8")
      val jsonNode: JsonNode = objectMapper.readTree(jsonStr)

      val id = Option(jsonNode.get("id"))
        .map(_.asText())
        .getOrElse(throw new IllegalArgumentException(s"'id' field missing in JSON: $jsonStr"))

      val wkt = Option(jsonNode.get("wkt"))
        .map(_.asText())
        .getOrElse(throw new IllegalArgumentException(s"'wkt' field missing in JSON: $jsonStr"))

      val producerTs = Option(jsonNode.get("timestamp"))
        .map(_.asLong())
        .getOrElse(throw new IllegalArgumentException(s"'timestamp' field missing in JSON: $jsonStr"))

      val event = Event(
        id = id,
        geometry = GeometryUtils.fromWKT(wkt),
        eventTime = TimeUtils.toMillis(producerTs),
        attributes = Map.empty
      )

      out.collect(event)
    }

    override def getProducedType: TypeInformation[Event] =
      TypeInformation.of(classOf[Event])
  }
}
