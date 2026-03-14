package org.schambon.loadsimrunner.generators;

import java.util.concurrent.atomic.AtomicLong;

public class MonotonicDistribution implements ShardDistribution {
    
    private final AtomicLong counter;
    private final long start;
    
    public MonotonicDistribution(long start) {
        this.start = start;
        this.counter = new AtomicLong(start);
    }
    
    @Override
    public long nextValue() {
        return counter.getAndIncrement();
    }
    
    @Override
    public String getType() {
        return "monotonic";
    }
}
