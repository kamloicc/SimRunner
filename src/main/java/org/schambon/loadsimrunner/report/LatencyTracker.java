package org.schambon.loadsimrunner.report;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

public class LatencyTracker {
    
    private final Recorder recorder;
    private Histogram intervalHistogram;
    
    public LatencyTracker() {
        this.recorder = new Recorder(3600000000L, 3);
        this.intervalHistogram = null;
    }
    
    public void recordLatency(long durationMillis) {
        long durationMicros = durationMillis * 1000;
        recorder.recordValue(durationMicros);
    }
    
    public LatencySnapshot getSnapshotAndReset() {
        Histogram histogram = recorder.getIntervalHistogram(intervalHistogram);
        intervalHistogram = histogram;
        
        if (histogram.getTotalCount() == 0) {
            return new LatencySnapshot(0, 0, 0, 0, 0, 0, 0);
        }
        
        return new LatencySnapshot(
            histogram.getValueAtPercentile(50.0) / 1000.0,
            histogram.getValueAtPercentile(90.0) / 1000.0,
            histogram.getValueAtPercentile(95.0) / 1000.0,
            histogram.getValueAtPercentile(99.0) / 1000.0,
            histogram.getValueAtPercentile(99.9) / 1000.0,
            histogram.getMaxValue() / 1000.0,
            histogram.getMean() / 1000.0
        );
    }
    
    public static class LatencySnapshot {
        public final double p50;
        public final double p90;
        public final double p95;
        public final double p99;
        public final double p999;
        public final double max;
        public final double mean;
        
        public LatencySnapshot(double p50, double p90, double p95, double p99, double p999, double max, double mean) {
            this.p50 = p50;
            this.p90 = p90;
            this.p95 = p95;
            this.p99 = p99;
            this.p999 = p999;
            this.max = max;
            this.mean = mean;
        }
    }
}
