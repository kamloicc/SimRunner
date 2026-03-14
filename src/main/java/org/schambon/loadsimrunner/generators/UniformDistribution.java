package org.schambon.loadsimrunner.generators;

import java.util.concurrent.ThreadLocalRandom;

public class UniformDistribution implements ShardDistribution {
    
    private final long min;
    private final long max;
    
    public UniformDistribution(long min, long max) {
        this.min = min;
        this.max = max;
    }
    
    @Override
    public long nextValue() {
        return ThreadLocalRandom.current().nextLong(min, max);
    }
    
    @Override
    public String getType() {
        return "uniform";
    }
}
