package cityrover.connect

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.kafka.common.serialization.{
  ByteArraySerializer,
  StringSerializer
}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import java.util
import scala.jdk.CollectionConverters.*


final class JsonToProtobufTask extends SinkTask {

  private var producer: KafkaProducer[String, Array[Byte]] = _
  private var outputTopic: String = _

  override def start(props: util.Map[String, String]): Unit = {

    outputTopic = props.get("output.topic")

    if (outputTopic == null || outputTopic.trim.isEmpty) {
      throw new ConnectException(
        "Missing required configuration: output.topic"
      )
    }

    val producerProps = new util.HashMap[String, Object]()

    producerProps.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      props.getOrDefault("bootstrap.servers", "kafka-1:19092")
    )

    producerProps.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer]
    )

    producerProps.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[ByteArraySerializer]
    )

    producer = new KafkaProducer[String, Array[Byte]](producerProps)
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {

    records.asScala.foreach { record =>

      if (record.value() == null) {
        throw new ConnectException(
          s"Null value received from topic '${record.topic()}'"
        )
      }

      val json = record.value() match {
        case value: String =>
          value

        case value =>
          throw new ConnectException(
            s"Expected String JSON value but received " +
              s"${value.getClass.getName} " +
              s"from topic '${record.topic()}'"
          )
      }

      val telemetry = JsonToTelemetry.parse(json) match {

        case Right(value) =>
          value

        case Left(error) =>
          throw new ConnectException(
            s"Failed to parse telemetry JSON from topic " +
              s"'${record.topic()}': $error"
          )
      }

      val protobufBytes =
        TelemetryProtobufEncoder.encode(telemetry)

      val key =
        Option(record.key())
          .map(_.toString)
          .getOrElse(telemetry.roverId)

      val producerRecord =
        new ProducerRecord[String, Array[Byte]](
          outputTopic,
          key,
          protobufBytes
        )

      producer.send(producerRecord)
    }

    producer.flush()
  }

  override def stop(): Unit = {

    if (producer != null) {
      producer.close()
      producer = null
    }
  }

  override def version(): String =
    "0.1.0"
}
