package org.schambon.loadsimrunner.sizing;

import java.util.Collection;

import org.bson.Document;
import org.schambon.loadsimrunner.report.Report;

public class WorkloadProfile {
    
    public final long durationSeconds;
    public final long avgReadsPerSec;
    public final long avgWritesPerSec;
    public final long peakReadsPerSec;
    public final long peakWritesPerSec;
    public final double avgP95Latency;
    public final double avgP99Latency;
    public final long totalDocuments;
    public final long avgDocumentSize;
    
    public WorkloadProfile(Collection<Report> reports, Long providedDocSize) {
        long totalReads = 0;
        long totalWrites = 0;
        long peakReads = 0;
        long peakWrites = 0;
        double totalP95 = 0;
        double totalP99 = 0;
        int reportCount = 0;
        long totalDocs = 0;
        long totalRecords = 0;
        
        for (Report report : reports) {
            Document reportDoc = report.getReport();
            
            for (String workload : reportDoc.keySet()) {
                Document metrics = (Document) reportDoc.get(workload);
                
                long ops = metrics.getLong("ops", 0L);
                long records = metrics.getLong("records", 0L);
                
                if (workload.toLowerCase().contains("insert") || 
                    workload.toLowerCase().contains("update") || 
                    workload.toLowerCase().contains("delete") ||
                    workload.toLowerCase().contains("replace")) {
                    totalWrites += ops;
                    peakWrites = Math.max(peakWrites, ops);
                } else {
                    totalReads += ops;
                    peakReads = Math.max(peakReads, ops);
                }
                
                totalP95 += metrics.getDouble("p95", 0.0);
                totalP99 += metrics.getDouble("p99", 0.0);
                totalDocs += metrics.getLong("total ops", 0L);
                totalRecords += metrics.getLong("total records", 0L);
                reportCount++;
            }
        }
        
        this.durationSeconds = reports.size() * 1;
        this.avgReadsPerSec = reportCount > 0 ? totalReads / reportCount : 0;
        this.avgWritesPerSec = reportCount > 0 ? totalWrites / reportCount : 0;
        this.peakReadsPerSec = peakReads;
        this.peakWritesPerSec = peakWrites;
        this.avgP95Latency = reportCount > 0 ? totalP95 / reportCount : 0;
        this.avgP99Latency = reportCount > 0 ? totalP99 / reportCount : 0;
        this.totalDocuments = totalDocs;
        
        if (providedDocSize != null) {
            this.avgDocumentSize = providedDocSize;
        } else {
            this.avgDocumentSize = totalRecords > 0 ? totalRecords / totalDocs : 1024;
        }
    }
}
