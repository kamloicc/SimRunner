package org.schambon.loadsimrunner.sizing;

public class SizingReport {
    
    public final WorkloadProfile workload;
    public final ResourceEstimate cluster;
    
    public SizingReport(WorkloadProfile workload, ResourceEstimate cluster) {
        this.workload = workload;
        this.cluster = cluster;
    }
    
    public static class ResourceEstimate {
        public final int shards;
        public final int nodesPerShard;
        public final int cpuPerNode;
        public final int ramGBPerNode;
        public final double storagePerNodeTB;
        public final double workingSetGB;
        public final double datasetSizeGB;
        public final double indexSizeGB;
        
        public ResourceEstimate(int shards, int nodesPerShard, int cpuPerNode, 
                               int ramGBPerNode, double storagePerNodeTB, 
                               double workingSetGB, double datasetSizeGB, double indexSizeGB) {
            this.shards = shards;
            this.nodesPerShard = nodesPerShard;
            this.cpuPerNode = cpuPerNode;
            this.ramGBPerNode = ramGBPerNode;
            this.storagePerNodeTB = storagePerNodeTB;
            this.workingSetGB = workingSetGB;
            this.datasetSizeGB = datasetSizeGB;
            this.indexSizeGB = indexSizeGB;
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("================================================================================\n");
        sb.append("CLUSTER SIZING RECOMMENDATION\n");
        sb.append("================================================================================\n\n");
        
        sb.append("Workload Analysis:\n");
        sb.append(String.format("  Duration: %d seconds\n", workload.durationSeconds));
        sb.append(String.format("  Avg reads/sec: %,d\n", workload.avgReadsPerSec));
        sb.append(String.format("  Avg writes/sec: %,d\n", workload.avgWritesPerSec));
        sb.append(String.format("  Peak reads/sec: %,d\n", workload.peakReadsPerSec));
        sb.append(String.format("  Peak writes/sec: %,d\n", workload.peakWritesPerSec));
        sb.append(String.format("  P95 latency: %.2fms\n", workload.avgP95Latency));
        sb.append(String.format("  P99 latency: %.2fms\n\n", workload.avgP99Latency));
        
        sb.append("Data Profile:\n");
        sb.append(String.format("  Total documents: %,d\n", workload.totalDocuments));
        sb.append(String.format("  Avg document size: %,d bytes\n", workload.avgDocumentSize));
        sb.append(String.format("  Dataset size: %.2f GB\n", cluster.datasetSizeGB));
        sb.append(String.format("  Estimated working set: %.2f GB\n", cluster.workingSetGB));
        sb.append(String.format("  Estimated index size: %.2f GB\n\n", cluster.indexSizeGB));
        
        sb.append("Recommended Cluster Configuration:\n");
        sb.append(String.format("  Shards: %d\n", cluster.shards));
        sb.append(String.format("  Nodes per shard: %d (Primary + %d Secondaries)\n", 
                  cluster.nodesPerShard, cluster.nodesPerShard - 1));
        sb.append(String.format("  Total nodes: %d\n\n", cluster.shards * cluster.nodesPerShard));
        
        sb.append("Per-Node Spec:\n");
        sb.append(String.format("  CPU: %d vCPU\n", cluster.cpuPerNode));
        sb.append(String.format("  RAM: %d GB\n", cluster.ramGBPerNode));
        sb.append(String.format("  Storage: %.1f GB NVMe (per node)\n\n", cluster.storagePerNodeTB * 1024));
        
        int totalCPU = cluster.shards * cluster.nodesPerShard * cluster.cpuPerNode;
        int totalRAM = cluster.shards * cluster.nodesPerShard * cluster.ramGBPerNode;
        double totalStorage = cluster.shards * cluster.nodesPerShard * cluster.storagePerNodeTB;
        
        sb.append("Total Cluster:\n");
        sb.append(String.format("  CPU: %d vCPU\n", totalCPU));
        sb.append(String.format("  RAM: %d GB\n", totalRAM));
        sb.append(String.format("  Storage: %.2f TB\n\n", totalStorage));
        
        sb.append("Sizing Notes:\n");
        sb.append("  - CPU sized for peak throughput with 25%% headroom\n");
        sb.append("  - RAM sized to fit working set + indexes + overhead\n");
        sb.append("  - Storage sized for data + indexes + oplog with replication\n");
        sb.append("  - Consider Atlas M30+ or equivalent instance types\n\n");
        
        sb.append("================================================================================\n");
        
        return sb.toString();
    }
}
