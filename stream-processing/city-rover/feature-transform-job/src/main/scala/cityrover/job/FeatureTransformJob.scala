package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import cityrover.pipeline.FeatureProcessingPipeline


object FeatureTransformJob:

  def main(args: Array[String]): Unit =
    // Allow passing parallelism as CLI argument
    val parallelism =
      if args.nonEmpty then args(0).toInt
      else 1

    // Create Flink environment
    val env: StreamExecutionEnvironment =
      FlinkEnvironment.create(parallelism)

    // Build the feature processing pipeline
    FeatureProcessingPipeline.build(env)

    // Execute the job
    env.execute("cityrover-feature-transform-job")

end FeatureTransformJob
