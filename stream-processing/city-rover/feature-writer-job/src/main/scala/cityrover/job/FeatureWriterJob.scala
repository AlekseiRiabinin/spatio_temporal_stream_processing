package cityrover.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import cityrover.pipeline.FeatureWriterPipeline
import cityrover.cassandra.CassandraConnectorConfig
import cityrover.util.ConfigLoader

object FeatureWriterJob:

  def main(args: Array[String]): Unit =
    // Determine parallelism (from env variable or default = 1)
    val parallelism =
      sys.env.get("FLINK_JOB_PARALLELISM").map(_.toInt).getOrElse(1)

    // Create Flink environment (restart strategy, checkpointing, object reuse)
    val env: StreamExecutionEnvironment =
      FlinkEnvironment.create(parallelism)

    // Cassandra configuration (from application.conf)
    val cassandraCfg =
      CassandraConnectorConfig(ConfigLoader.rawConfig)

    // Build pipeline: Kafka (protobuf enriched) → Cassandra
    FeatureWriterPipeline.build(env, cassandraCfg)

    // Execute job
    env.execute("cityrover-feature-writer")

end FeatureWriterJob
