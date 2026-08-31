package cityrover.connect

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util


class JsonToProtobufConnector extends SinkConnector {

  private var props: util.Map[String, String] = _

  override def start(props: util.Map[String, String]): Unit = {
    this.props = props
  }

  override def taskClass(): Class[_ <: Task] = {
    classOf[JsonToProtobufTask]
  }

  override def taskConfigs(
      maxTasks: Int
  ): util.List[util.Map[String, String]] = {

    val configs =
      new util.ArrayList[util.Map[String, String]](maxTasks)

    var i = 0
    while (i < maxTasks) {
      configs.add(new util.HashMap[String, String](props))
      i += 1
    }

    configs
  }

  override def stop(): Unit = {
    props = null
  }

  override def config(): ConfigDef = {
    JsonToProtobufConnector.CONFIG_DEF
  }

  override def version(): String = {
    JsonToProtobufConnector.VERSION
  }
}

object JsonToProtobufConnector {

  val VERSION = "0.1.0"

  val CONFIG_DEF: ConfigDef =
    new ConfigDef()
}
