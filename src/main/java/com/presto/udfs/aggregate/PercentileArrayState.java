package com.presto.udfs.aggregate;

import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateMetadata;

import java.util.List;
import java.util.Map;

@AccumulatorStateMetadata(stateSerializerClass = PercentileArrayStateSerializer.class, stateFactoryClass = PercentileArrayStateFactory.class)
public interface PercentileArrayState extends AccumulatorState {

    Map<Long, Long> getCounts();

    void setCounts(Map<Long, Long> counts);

    List<Double> getPercentiles();

    void setPercentiles(List<Double> percentiles);

    void addMemoryUsage(int value);
}
