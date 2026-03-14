package org.schambon.loadsimrunner.report;

import static java.lang.System.currentTimeMillis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Reporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(Reporter.class);

    private volatile Map<String, StatsHolder> stats = null;
    private long startTime = 0;
    private TreeMap<Instant, Report> reports = new TreeMap<>();
    private List<Integer> percentiles;

    public Reporter(List<Integer> reportPercentiles) {
        this.percentiles = reportPercentiles;
    }

    public void start() {
        stats = new TreeMap<>();
        startTime = System.currentTimeMillis();
    }

    public void reportInit(String message) {
        LOGGER.info(String.format("INIT: %s", message));
    }

    public void computeReport(List<? extends ReporterCallback> callbacks) {
        LOGGER.debug("Scheduling report compute");
        asyncExecutor.submit(() -> {
            try {
                LOGGER.debug("Running report compute");
                var oldStats = stats;
                long now = System.currentTimeMillis();
                long interval = now - startTime;
                startTime = now;
        
                stats = new TreeMap<>();
        
                Document reportDoc = new Document();
                for (var workload: oldStats.keySet()) {
                    Document computedStats = oldStats.get(workload).compute(interval, percentiles);
                    if (computedStats != null) {
                        reportDoc.append(workload, computedStats);
                    }
                }
        
                Instant reportInstant = Instant.ofEpochMilli(now);
                Report report = new Report(reportInstant, reportDoc);
                synchronized(this) {
                    reports.put(reportInstant, report);
                    // evict reports older than one hour
                    var oneHourAgo = reportInstant.minus(Duration.ofHours(1));
                    reports.headMap(oneHourAgo).clear();
                }
        
                LOGGER.info(report.toString());

                for (var cb : callbacks) {
                    cb.report(report);
                }

            } catch (Throwable t) {
                LOGGER.error("Error while computing report", t);
            }
        });
    }

    public synchronized void reportOp(String name, long i, long duration) {
        //LOGGER.debug("Reported {} {} {}", name, i, duration);
        StatsHolder h = stats.get(name);
        if (h == null) {
            h = new StatsHolder();
            stats.put(name, h);
        }
        h.addOp(i, duration);
    }

    public Collection<Report> getAllReports() {
        return reports.values();
    }

    public synchronized Collection<Report> getReportsSince(Instant start) {
        return reports.tailMap(start, false).values();
    }
    
    // a specific thread for logging durations
    static ExecutorService asyncExecutor = Executors.newFixedThreadPool(1);

    private static class StatsHolder {

        AtomicLong numOps = new AtomicLong(0);
        AtomicLong totalRecords = new AtomicLong(0);
        AtomicLong totalDuration = new AtomicLong(0);
        LatencyTracker latencyTracker = new LatencyTracker();

        public Document compute(long interval, List<Integer> percentiles) {
            var __startCompute = currentTimeMillis();

            long ops = numOps.getAndSet(0);
            long records = totalRecords.getAndSet(0);
            long duration = totalDuration.getAndSet(0);
            
            LatencyTracker.LatencySnapshot snapshot = latencyTracker.getSnapshotAndReset();

            if (ops == 0) {
                return null;
            }

            long opsPerSec = (long) (ops / ((double)interval/1000.0));
            long recordsPerSec = (long) (records / ((double)interval/1000.0));
            double meanBatchSize = records / (double) ops;
            double util = 100.0 * duration / (double) interval;

            if (opsPerSec > 1e10) {
                LOGGER.warn("Computed very large ops number {}. Ops: {}, interval: {}", opsPerSec, ops, interval);
                return null;
            }

            Document wlReport = new Document();
            wlReport.append("ops", opsPerSec);
            wlReport.append("records", recordsPerSec);
            wlReport.append("total ops", ops);
            wlReport.append("total records", records);
            wlReport.append("mean duration", snapshot.mean);
            wlReport.append("p50", snapshot.p50);
            wlReport.append("p90", snapshot.p90);
            wlReport.append("p95", snapshot.p95);
            wlReport.append("p99", snapshot.p99);
            wlReport.append("p999", snapshot.p999);
            wlReport.append("max", snapshot.max);
            wlReport.append("mean batch size", meanBatchSize);
            wlReport.append("client util", util);
            wlReport.append("report compute time", currentTimeMillis() - __startCompute);

            return wlReport;
        }

        public void addOp(long number, long duration) {
            numOps.incrementAndGet();
            totalRecords.addAndGet(number);
            totalDuration.addAndGet(duration);
            latencyTracker.recordLatency(duration);
        }
    }
}
