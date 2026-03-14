package org.schambon.loadsimrunner.report;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;

public class Report {

    private Instant time;
    private Document report;
    
    public Report(Instant time, Document report) {
        this.time = time;
        this.report = report;
    }

    public Document getReport() {
        return report;
    }

    public Instant getTime() {
        return time;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(time.toString());
        for (var entry: report.entrySet()) {
            sb.append("\n");
            sb.append(workloadReport(entry.getKey(), (Document) entry.getValue()));
        }
        return sb.toString();
    }

    private String workloadReport(String name, Document wlReport) {
        return String.format("%s:\n==========\n%d ops per second (%d total)\n%d records per second (%d total)\nLatency: p50=%.2fms, p90=%.2fms, p95=%.2fms, p99=%.2fms, p999=%.2fms, max=%.2fms (mean=%.2fms)\n%.2f Batch size avg\n[util %%: %.2f -- report computed in %d]",
            name,
            wlReport.getLong("ops"), wlReport.getLong("total ops"),
            wlReport.getLong("records"), wlReport.getLong("total records"),
            wlReport.getDouble("p50"),
            wlReport.getDouble("p90"),
            wlReport.getDouble("p95"),
            wlReport.getDouble("p99"),
            wlReport.getDouble("p999"),
            wlReport.getDouble("max"),
            wlReport.getDouble("mean duration"),
            wlReport.getDouble("mean batch size"),
            wlReport.getDouble("client util"),
            wlReport.getLong("report compute time")
        );
    }

    public String toJSON() {
        return new Document("time", time.toString()).append("report", report).toJson();
    }
    
}
