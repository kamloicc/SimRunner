package org.schambon.loadsimrunner.generators;

public interface ShardDistribution {
    long nextValue();
    String getType();
}
