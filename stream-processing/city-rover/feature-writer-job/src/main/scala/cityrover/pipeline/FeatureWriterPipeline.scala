package cityrover.pipeline

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.eventtime.WatermarkStrategy

import cityrover.kafka.KafkaSourceEnriched
import cityrover.cassandra.{CassandraSink, CassandraConnectorConfig}
import cityrover.telemetry.EnrichedTelemetryEvent

import cityrover.metrics.{StampedRecord, TimestampTagger}
import cityrover.util.MetricsConfig


/**
 * FeatureWriterPipeline
 *
 * Reads enriched telemetry events (protobuf) from Kafka
 * and writes them directly to Cassandra.
 *
 * No transformations, no windowing. The pipeline is:
 *
 *   KafkaSourceEnriched → TimestampTagger → CassandraSink
 *
 * The TimestampTagger stamps each record with t_src_ms — the wall clock
 * at the moment the record leaves the source operator. The CassandraSink
 * then measures and records the latency decomposition via Prometheus:
 *
 *   kafka_to_flink_ms = t_src_ms         - t1_ms
 *   flink_ms          = t_write_start_ms - t_src_ms
 *   write_ms          = t2_ms            - t_write_start_ms
 *   latency_ms        = t2_ms            - t1_ms
 *
 * This mirrors the metric decomposition from the PyFlink probe
 * cassandra_latency_probe.py.
 *
 * The metrics config is threaded through from FeatureWriterJob, so the
 * toggles in application.conf determine which histograms get registered.
 */
object FeatureWriterPipeline:

  def build(
    env: StreamExecutionEnvironment,
    cassandraCfg: CassandraConnectorConfig,
    metricsCfg: MetricsConfig
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
    // Source-boundary instrumentation.
    //
    //    Wraps every EnrichedTelemetryEvent into a StampedRecord that
    //    carries:
    //      - payload : the original EnrichedTelemetryEvent
    //      - t1Ms    : -1 (not available at map time; the sink falls back
    //                  to skipping metrics that require t1)
    //      - tSrcMs  : wall clock when the record left the source
    //
    //    The subgroup "writer" determines the Prometheus metric names:
    //      flink_taskmanager_job_task_operator_cityrover_writer_<metric>
    // --------------------------------------------------------------------
    val stampedStream =
      enrichedStream.map(
        TimestampTagger[EnrichedTelemetryEvent](
          metricsCfg,
          subgroup = "writer"
        )
      )

    // --------------------------------------------------------------------
    // Final sink: Cassandra.
    //
    //    The sink unwraps the StampedRecord to write the payload, and
    //    records the latency histograms around session.executeAsync().
    // --------------------------------------------------------------------
    stampedStream.sinkTo(
      new CassandraSink(cassandraCfg, metricsCfg)
    )

end FeatureWriterPipeline
