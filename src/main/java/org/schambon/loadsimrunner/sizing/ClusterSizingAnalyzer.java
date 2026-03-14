package org.schambon.loadsimrunner.sizing;

import java.util.Collection;

import org.schambon.loadsimrunner.report.Report;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterSizingAnalyzer {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSizingAnalyzer.class);
    
    private final SizingConfig config;
    
    public ClusterSizingAnalyzer(SizingConfig config) {
        this.config = config;
    }
    
    public SizingReport analyze(Collection<Report> reports) {
        if (reports.isEmpty()) {
            LOGGER.warn("No reports available for sizing analysis");
            return null;
        }
        
        WorkloadProfile profile = new WorkloadProfile(reports, 
            config.avgDocSizeBytes != null ? config.avgDocSizeBytes.longValue() : null);
        
        SizingReport.ResourceEstimate estimate = estimateResources(profile);
        
        return new SizingReport(profile, estimate);
    }
    
    private SizingReport.ResourceEstimate estimateResources(WorkloadProfile profile) {
        long peakTotalOps = profile.peakReadsPerSec + (long)(profile.peakWritesPerSec * 1.5);
        
        int requiredCores = (int) Math.ceil((double) peakTotalOps * 1.25 / config.opsPerCore);
        
        double datasetSizeGB = calculateDatasetSize(profile);
        double indexSizeGB = datasetSizeGB * config.indexRatio;
        double workingSetGB = calculateWorkingSet(profile, datasetSizeGB);
        
        int ramGBPerNode = calculateRAMPerNode(workingSetGB, indexSizeGB);
        
        int shardsByThroughput = (int) Math.ceil((double) peakTotalOps / (config.opsPerCore * 8));
        int shardsByData = (int) Math.ceil(datasetSizeGB / (config.maxShardSizeTB * 1024));
        int shards = Math.max(1, Math.max(shardsByThroughput, shardsByData));
        
        int nodesPerShard = config.replicationFactor;
        
        int cpuPerNode = Math.max(4, (int) Math.ceil((double) requiredCores / shards));
        cpuPerNode = roundToPowerOf2(cpuPerNode);
        
        double oplogSizeGB = datasetSizeGB * 0.05;
        double storagePerNodeGB = (datasetSizeGB + indexSizeGB + oplogSizeGB) / shards * config.replicationFactor;
        storagePerNodeGB *= 1.2;
        double storagePerNodeTB = storagePerNodeGB / 1024.0;
        
        return new SizingReport.ResourceEstimate(
            shards,
            nodesPerShard,
            cpuPerNode,
            ramGBPerNode,
            storagePerNodeTB,
            workingSetGB,
            datasetSizeGB,
            indexSizeGB
        );
    }
    
    private double calculateDatasetSize(WorkloadProfile profile) {
        if (config.estimatedDocsMillions != null && config.avgDocSizeBytes != null) {
            return (config.estimatedDocsMillions * 1_000_000.0 * config.avgDocSizeBytes) / (1024.0 * 1024.0 * 1024.0);
        }
        
        return (profile.totalDocuments * profile.avgDocumentSize) / (1024.0 * 1024.0 * 1024.0);
    }
    
    private double calculateWorkingSet(WorkloadProfile profile, double datasetSizeGB) {
        if (config.workingSetGB != null) {
            return config.workingSetGB;
        }
        
        double readWriteRatio = profile.avgReadsPerSec > 0 ? 
            (double) profile.avgReadsPerSec / (profile.avgReadsPerSec + profile.avgWritesPerSec) : 0.5;
        
        if (readWriteRatio > 0.8) {
            return Math.min(datasetSizeGB * 0.3, datasetSizeGB);
        } else if (readWriteRatio > 0.5) {
            return Math.min(datasetSizeGB * 0.5, datasetSizeGB);
        } else {
            return Math.min(datasetSizeGB * 0.7, datasetSizeGB);
        }
    }
    
    private int calculateRAMPerNode(double workingSetGB, double indexSizeGB) {
        double requiredRAM = (workingSetGB + indexSizeGB) * 1.2 + 4;
        
        int ramGB = (int) Math.ceil(requiredRAM);
        
        ramGB = Math.max(8, ramGB);
        
        int[] standardSizes = {8, 16, 32, 64, 128, 256, 512};
        for (int size : standardSizes) {
            if (ramGB <= size) {
                return size;
            }
        }
        return 512;
    }
    
    private int roundToPowerOf2(int value) {
        int[] powers = {2, 4, 8, 16, 32, 64, 128};
        for (int p : powers) {
            if (value <= p) {
                return p;
            }
        }
        return 128;
    }
}
