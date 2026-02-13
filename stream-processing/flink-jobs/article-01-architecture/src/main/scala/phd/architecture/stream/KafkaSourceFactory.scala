package phd.architecture.stream

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.util.Properties

import phd.architecture.model.Event
import phd.architecture.model.TypeInfos._
import phd.architecture.util.{GeometryUtils, TimeUtils}

object KafkaSourceFactory {

  /**
   * Creates a Kafka source producing spatial-temporal events.
   *
   * Expected message format (example JSON):
   * {
   *   "id": "obj-1",
   *   "wkt": "POINT(30 10)",
   *   "timestamp": 1699999999999,
   *   "attributes": { "speed": "45" }
   * }
   */
  def createSpatialEventSource(env: StreamExecutionEnvironment): DataStream[Event] = {

    val props = new Properties()
    props.setProperty("bootstrap.servers", "kafka-1:19092,kafka-2:19094")
    props.setProperty("group.id", "article-01-architecture")

    val consumer =
      new FlinkKafkaConsumer[Event](
        "spatial-events",
        new EventDeserializationSchema,
        props
      )

    env.addSource(consumer)
  }

  private class EventDeserializationSchema
      extends DeserializationSchema[Event]
      with Serializable {

    override def deserialize(message: Array[Byte]): Event = {

      val json = new String(message, "UTF-8")

      val id = extract(json, "id")
      val wkt = extract(json, "wkt")
      val producerTs = extract(json, "timestamp").toLong

      // Kafka append timestamp is provided by Flink
      // recordTimestamp is passed via deserialize(record, recordTimestamp)
      // but Flink's simple schema does not expose it directly.
      // Instead, we use System.currentTimeMillis() as an approximation.
      val kafkaTs = System.currentTimeMillis()

      val ingestLatency = kafkaTs - producerTs

      println(
        s"[ingest-latency] id=$id producerTs=$producerTs kafkaTs=$kafkaTs latencyMs=$ingestLatency"
      )

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


// docker logs -f flink-taskmanager
