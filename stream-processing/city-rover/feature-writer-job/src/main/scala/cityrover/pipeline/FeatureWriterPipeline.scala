package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.eventtime.WatermarkStrategy

import cityrover.kafka.KafkaSourceEnriched
import cityrover.cassandra.{CassandraSink, CassandraConnectorConfig}
import cityrover.telemetry.EnrichedTelemetryEvent


/**
  * FeatureWriterPipeline
  *
  * Reads enriched telemetry events (protobuf) from Kafka
  * and writes them directly to Cassandra.
  *
  * No transformations, no windowing, no metrics — this job
  * is purely a materialization pipeline.
  */
object FeatureWriterPipeline:

  def build(
    env: StreamExecutionEnvironment,
    cassandraCfg: CassandraConnectorConfig
  ): Unit =

    // --------------------------------------------------------------------
    // Kafka source: enriched telemetry (protobuf)
    // --------------------------------------------------------------------
    val enrichedStream =
      env.fromSource(
        KafkaSourceEnriched.source(),
        WatermarkStrategy.noWatermarks(),
        "enriched-telemetry-source"
      )

    // --------------------------------------------------------------------
    // Final sink: Cassandra
    // --------------------------------------------------------------------
    enrichedStream.sinkTo(new CassandraSink(cassandraCfg))

end FeatureWriterPipeline
