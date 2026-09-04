package cityrover.metrics

import org.apache.flink.metrics.prometheus.PrometheusReporter
import org.apache.flink.configuration.Configuration


object PrometheusSetup:

  def config: Configuration =
    val cfg = Configuration()

    // Enable Prometheus reporter
    cfg.setString("metrics.reporters", "prometheus")

    // Reporter class
    cfg.setString(
      "metrics.reporter.prometheus.class",
      classOf[PrometheusReporter].getName
    )

    // Expose metrics via HTTP endpoint
    cfg.setString(
      "metrics.reporter.prometheus.port",
      "9090"
    )

    // Enable JVM metrics (GC, memory, threads)
    cfg.setString(
      "metrics.system-resource",
      "true"
    )

    cfg

end PrometheusSetup
