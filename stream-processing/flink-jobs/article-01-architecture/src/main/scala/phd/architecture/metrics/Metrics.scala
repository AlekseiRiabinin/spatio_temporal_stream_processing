package phd.architecture.metrics

import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import io.prometheus.client.{Counter, Gauge, Histogram}


object Metrics {

  // Start Prometheus endpoint once
  def init(): Unit = {
    DefaultExports.initialize()
    new HTTPServer(9000)
  }

  // -----------------------------
  // Metric definitions
  // -----------------------------

  val eventsConsumed: Counter = Counter.build()
    .name("events_consumed_total")
    .help("Total number of events consumed from Kafka")
    .register()

  val ingestionLatency: Histogram = Histogram.build()
    .name("ingestion_latency_seconds")
    .help("Kafka → Flink ingestion latency")
    .register()

  val watermarkLag: Gauge = Gauge.build()
    .name("watermark_lag_ms")
    .help("Current watermark lag in milliseconds")
    .register()

  val windowLatency: Histogram = Histogram.build()
    .name("window_processing_latency_seconds")
    .help("Latency of window processing")
    .register()

  val processingLatency: Gauge = Gauge.build()
    .name("processing_latency_ms")
    .help("End-to-end latency after windowing")
    .register()
}
