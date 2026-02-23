package phd.streammodels.stream

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

import phd.streammodels.model.Event
import phd.streammodels.model.TypeInfos._
import phd.streammodels.util.{GeometryUtils, TimeUtils}


object KafkaSourceFactory {

  private val objectMapper = new ObjectMapper()

  /**
    * Generic Kafka source for Article 2 (no spatial partitioning).
    */
  def createEventSource(env: StreamExecutionEnvironment): DataStream[Event] = {

    val source =
      KafkaSource.builder[Event]()
        .setBootstrapServers("kafka-1:19092")
        .setTopics("spatial-events")
        .setGroupId("article-02-stream-models")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setDeserializer(new EventKafkaRecordDeserializationSchema)
        .build()

    env.fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "KafkaSource(events)"
    )
  }

  /**
    * Minimal Kafka record deserializer.
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

      val id = jsonNode.get("id").asText()
      val wkt = jsonNode.get("wkt").asText()
      val producerTs = jsonNode.get("timestamp").asLong()

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
