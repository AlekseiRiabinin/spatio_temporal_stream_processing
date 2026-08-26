package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import cityrover.pipeline.ProcessingPipeline
import cityrover.metrics.PrometheusSetup


object LatencyResearchJob {

  def main(args: Array[String]): Unit = {

    // Prometheus metrics configuration
    val cfg = PrometheusSetup.config

    // Create Flink environment with metrics enabled
    val env = StreamExecutionEnvironment.getExecutionEnvironment(cfg)

    // Build the latency research pipeline
    ProcessingPipeline.build(env)

    // Execute the job
    env.execute("cityrover-latency-research")
  }
}
