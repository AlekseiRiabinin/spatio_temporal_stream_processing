package phd.architecture.metrics

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import java.time.Instant


object MetricsRegistry {

  private val latency = ListBuffer.empty[String]
  private val windows = ListBuffer.empty[String]
  private val watermarks = ListBuffer.empty[String]
  private val partitions = ListBuffer.empty[String]

  def recordLatency(value: String): Unit =
    synchronized { latency += s"${Instant.now.toEpochMilli}: $value" }

  def recordWindow(value: String): Unit =
    synchronized { windows += s"${Instant.now.toEpochMilli}: $value" }

  def recordWatermark(value: String): Unit =
    synchronized { watermarks += s"${Instant.now.toEpochMilli}: $value" }

  def recordPartition(value: String): Unit =
    synchronized { partitions += s"${Instant.now.toEpochMilli}: $value" }

  def toJson: String =
    s"""
       |{
       |  "latency": ${latency.map("\"" + _ + "\"").mkString("[", ",", "]")},
       |  "windows": ${windows.map("\"" + _ + "\"").mkString("[", ",", "]")},
       |  "watermarks": ${watermarks.map("\"" + _ + "\"").mkString("[", ",", "]")},
       |  "partitions": ${partitions.map("\"" + _ + "\"").mkString("[", ",", "]")}
       |}
       |""".stripMargin
}
