package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.configuration.Configuration

import cityrover.pipeline.ProcessingPipeline
import cityrover.metrics.PrometheusSetup


object LatencyResearchJob {

  def main(args: Array[String]): Unit = {

    val cfg: Configuration = PrometheusSetup.config

    val env = StreamExecutionEnvironment.getExecutionEnvironment(cfg)

    ProcessingPipeline.build(env)

    env.execute("cityrover-latency-research")
  }
}
