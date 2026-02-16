package phd.architecture.stream

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.Properties

import phd.architecture.model.Event
import phd.architecture.model.TypeInfos._
import phd.architecture.util.{GeometryUtils, TimeUtils}
import phd.architecture.metrics.Metrics


object KafkaSourceFactory {

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

    override def deserialize(message: Array[Byte]): Event = {

      val json = new String(message, "UTF-8")

      val id = extract(json, "id")
      val wkt = extract(json, "wkt")
      val producerTs = extract(json, "timestamp").toLong

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

    private def extract(json: String, field: String): String = {
      val pattern = (""""""" + field + """":\s*"?([^",}]+)"?""").r
      pattern.findFirstMatchIn(json).get.group(1)
    }
  }
}
