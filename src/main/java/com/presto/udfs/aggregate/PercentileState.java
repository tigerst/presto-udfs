package com.presto.udfs.aggregate;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.Map;

@AccumulatorStateMetadata(stateSerializerClass = PercentileStateSerializer.class, stateFactoryClass = PercentileStateFactory.class)
public interface PercentileState extends AccumulatorState {

    Map<Long, Long> getCounts();

    void setCounts(Map<Long, Long> counts);

    double getPercentile();

    void setPercentile(double precentile);

    void addMemoryUsage(int value);
}
