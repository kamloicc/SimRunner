package org.schambon.loadsimrunner.generators;

import java.util.concurrent.ThreadLocalRandom;

public class ZipfianDistribution implements ShardDistribution {
    
    private final long min;
    private final long range;
    private final double exponent;
    
    public ZipfianDistribution(long min, long max, double exponent) {
        this.min = min;
        this.range = max - min;
        this.exponent = exponent;
    }
    
    @Override
    public long nextValue() {
        double uniform = ThreadLocalRandom.current().nextDouble();
        double zipf = Math.pow(uniform, exponent);
        long value = min + (long)(zipf * range);
        return Math.min(value, min + range - 1);
    }
    
    @Override
    public String getType() {
        return "zipfian";
    }
}
