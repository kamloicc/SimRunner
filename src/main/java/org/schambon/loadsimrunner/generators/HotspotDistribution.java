package org.schambon.loadsimrunner.generators;

import java.util.concurrent.ThreadLocalRandom;

public class HotspotDistribution implements ShardDistribution {
    
    private final long min;
    private final long max;
    private final long hotspotMin;
    private final long hotspotMax;
    private final double hotspotProbability;
    
    public HotspotDistribution(long min, long max, double hotspotPercentage, double hotspotProbability) {
        this.min = min;
        this.max = max;
        this.hotspotProbability = hotspotProbability;
        
        long range = max - min;
        long hotspotRange = (long)(range * (hotspotPercentage / 100.0));
        this.hotspotMin = min;
        this.hotspotMax = min + hotspotRange;
    }
    
    @Override
    public long nextValue() {
        if (ThreadLocalRandom.current().nextDouble() < hotspotProbability) {
            return ThreadLocalRandom.current().nextLong(hotspotMin, hotspotMax);
        } else {
            return ThreadLocalRandom.current().nextLong(min, max);
        }
    }
    
    @Override
    public String getType() {
        return "hotspot";
    }
}
