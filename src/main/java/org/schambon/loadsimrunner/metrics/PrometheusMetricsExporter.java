package org.schambon.loadsimrunner.metrics;

import java.io.IOException;

import org.bson.Document;
import org.schambon.loadsimrunner.report.Report;
import org.schambon.loadsimrunner.report.ReporterCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;

public class PrometheusMetricsExporter implements ReporterCallback {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusMetricsExporter.class);
    
    private final HTTPServer server;
    private final boolean enabled;
    
    private static final Counter opsTotal = Counter.build()
        .name("simrunner_ops_total")
        .help("Total operations executed")
        .labelNames("workload")
        .register();
    
    private static final Counter recordsTotal = Counter.build()
        .name("simrunner_records_total")
        .help("Total records processed")
        .labelNames("workload")
        .register();
    
    private static final Gauge latencyP50 = Gauge.build()
        .name("simrunner_latency_p50_ms")
        .help("50th percentile latency in milliseconds")
        .labelNames("workload")
        .register();
    
    private static final Gauge latencyP90 = Gauge.build()
        .name("simrunner_latency_p90_ms")
        .help("90th percentile latency in milliseconds")
        .labelNames("workload")
        .register();
    
    private static final Gauge latencyP95 = Gauge.build()
        .name("simrunner_latency_p95_ms")
        .help("95th percentile latency in milliseconds")
        .labelNames("workload")
        .register();
    
    private static final Gauge latencyP99 = Gauge.build()
        .name("simrunner_latency_p99_ms")
        .help("99th percentile latency in milliseconds")
        .labelNames("workload")
        .register();
    
    private static final Gauge latencyP999 = Gauge.build()
        .name("simrunner_latency_p999_ms")
        .help("99.9th percentile latency in milliseconds")
        .labelNames("workload")
        .register();
    
    private static final Gauge latencyMax = Gauge.build()
        .name("simrunner_latency_max_ms")
        .help("Maximum latency in milliseconds")
        .labelNames("workload")
        .register();
    
    private static final Gauge opsPerSecond = Gauge.build()
        .name("simrunner_ops_per_second")
        .help("Operations per second")
        .labelNames("workload")
        .register();
    
    private static final Gauge recordsPerSecond = Gauge.build()
        .name("simrunner_records_per_second")
        .help("Records per second")
        .labelNames("workload")
        .register();
    
    public PrometheusMetricsExporter(Document config) {
        if (config == null || !config.getBoolean("enabled", false)) {
            this.enabled = false;
            this.server = null;
            LOGGER.info("Prometheus metrics exporter disabled");
            return;
        }
        
        this.enabled = true;
        int port = config.getInteger("port", 9090);
        
        try {
            this.server = new HTTPServer(port);
            LOGGER.info("Prometheus metrics server started on port {}", port);
        } catch (IOException e) {
            LOGGER.error("Failed to start Prometheus metrics server on port {}", port, e);
            throw new RuntimeException("Cannot start Prometheus metrics server", e);
        }
    }
    
    @Override
    public void report(Report report) {
        if (!enabled) {
            return;
        }
        
        Document reportDoc = report.getReport();
        
        for (String workload : reportDoc.keySet()) {
            Document metrics = (Document) reportDoc.get(workload);
            
            opsTotal.labels(workload).inc(metrics.getLong("total ops", 0L));
            recordsTotal.labels(workload).inc(metrics.getLong("total records", 0L));
            
            opsPerSecond.labels(workload).set(metrics.getLong("ops", 0L));
            recordsPerSecond.labels(workload).set(metrics.getLong("records", 0L));
            
            latencyP50.labels(workload).set(metrics.getDouble("p50", 0.0));
            latencyP90.labels(workload).set(metrics.getDouble("p90", 0.0));
            latencyP95.labels(workload).set(metrics.getDouble("p95", 0.0));
            latencyP99.labels(workload).set(metrics.getDouble("p99", 0.0));
            latencyP999.labels(workload).set(metrics.getDouble("p999", 0.0));
            latencyMax.labels(workload).set(metrics.getDouble("max", 0.0));
        }
    }
    
    public void stop() {
        if (server != null) {
            server.close();
            LOGGER.info("Prometheus metrics server stopped");
        }
    }
}
