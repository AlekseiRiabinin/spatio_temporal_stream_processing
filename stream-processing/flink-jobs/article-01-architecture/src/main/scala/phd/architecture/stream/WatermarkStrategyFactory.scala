package phd.architecture.stream

import org.apache.flink.api.common.eventtime._
import phd.architecture.model.Event
import java.time.Duration


object WatermarkStrategyFactory {

  def eventTimeWatermarks(maxOutOfOrdernessSeconds: Long): WatermarkStrategy[Event] = {

    WatermarkStrategy
      .forBoundedOutOfOrderness[Event](
        Duration.ofSeconds(maxOutOfOrdernessSeconds)
      )
      .withTimestampAssigner(
        new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(
            element: Event,
            recordTimestamp: Long
          ): Long =
            element.eventTime
        }
      )
  }
}
