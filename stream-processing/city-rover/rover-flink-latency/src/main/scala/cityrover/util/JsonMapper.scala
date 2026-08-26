package cityrover.util

import com.fasterxml.jackson.databind.{
  ObjectMapper,
  SerializationFeature,
  DeserializationFeature
}
import com.fasterxml.jackson.module.scala.DefaultScalaModule


object JsonMapper {

  private val mapper: ObjectMapper =
    new ObjectMapper()
      .registerModule(DefaultScalaModule)
      .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson[T](value: T): String =
    mapper.writeValueAsString(value)

  def fromJson[T](json: String, clazz: Class[T]): T =
    mapper.readValue(json, clazz)
}
