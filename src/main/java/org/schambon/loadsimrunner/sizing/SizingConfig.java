package org.schambon.loadsimrunner.sizing;

import org.bson.Document;

public class SizingConfig {
    
    public final boolean enabled;
    public final int opsPerCore;
    public final double indexRatio;
    public final int replicationFactor;
    public final double maxShardSizeTB;
    public final Double workingSetGB;
    public final Integer avgDocSizeBytes;
    public final Long estimatedDocsMillions;
    public final boolean periodic;
    
    public SizingConfig(Document config) {
        if (config == null) {
            this.enabled = false;
            this.opsPerCore = 4000;
            this.indexRatio = 0.3;
            this.replicationFactor = 3;
            this.maxShardSizeTB = 2.0;
            this.workingSetGB = null;
            this.avgDocSizeBytes = null;
            this.estimatedDocsMillions = null;
            this.periodic = false;
            return;
        }
        
        this.enabled = config.getBoolean("enabled", false);
        this.opsPerCore = config.getInteger("opsPerCore", 4000);
        this.indexRatio = config.getDouble("indexRatio", 0.3);
        this.replicationFactor = config.getInteger("replicationFactor", 3);
        this.maxShardSizeTB = config.getDouble("maxShardSizeTB", 2.0);
        this.workingSetGB = config.getDouble("workingSetGB");
        this.avgDocSizeBytes = config.getInteger("avgDocSizeBytes");
        this.estimatedDocsMillions = config.getLong("estimatedDocsMillions");
        this.periodic = config.getBoolean("periodic", false);
    }
}
