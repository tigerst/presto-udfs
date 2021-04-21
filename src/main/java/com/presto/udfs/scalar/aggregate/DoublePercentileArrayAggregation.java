package com.presto.udfs.scalar.aggregate;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.aggregate.PercentileArrayState;
import com.presto.udfs.aggregate.PercentileCommon;

import java.util.*;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("percentile_v2")
public final class DoublePercentileArrayAggregation {

    @InputFunction
    public static void input(@AggregationState PercentileArrayState state,
                             @SqlType(StandardTypes.DOUBLE) double value,
                             @SqlType("array(double)") Block percentilesArrayBlock) {
        LongPercentileArrayAggregation.input(state, PercentileCommon.doubleToSortableLong(value), percentilesArrayBlock);
    }

    @CombineFunction
    public static void combine(@AggregationState PercentileArrayState state,
                               PercentileArrayState otherState) {
        LongPercentileArrayAggregation.combine(state, otherState);
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState PercentileArrayState state, BlockBuilder out) {
        List<Double> percentiles = state.getPercentiles();
        Map<Long, Long> counts = state.getCounts();
        if (counts == null) {
            out.appendNull();
            return;
        } else {

            // Get all items into an array and sort them.
            Set<Map.Entry<Long, Long>> entries = state.getCounts().entrySet();
            List<Map.Entry<Long, Long>> entriesList = new ArrayList<>();
            entries.stream().forEach(entry -> {
                entriesList.add(new AbstractMap.SimpleEntry(entry.getKey(), entry.getValue()));
            });

            Collections.sort(entriesList, new PercentileCommon.MyComparator());

            // Accumulate the counts.
            long total = 0;
            for (int i = 0; i < entriesList.size(); i++) {
                long count = entriesList.get(i).getValue();
                total += count;
                entriesList.get(i).setValue(total);
            }

            // maxPosition is the 1.0 percentile
            long maxPosition = total - 1;

            BlockBuilder blockBuilder = out.beginBlockEntry();
            for (int i = 0; i < percentiles.size(); i++) {
                Double percentile = percentiles.get(i);
                double position = maxPosition * percentile;
                DOUBLE.writeDouble(blockBuilder, PercentileCommon.sortableLongToDouble(Double.valueOf(PercentileCommon.getPercentile(entriesList, position)).longValue()));
            }
            out.closeEntry();

        }
    }

}
