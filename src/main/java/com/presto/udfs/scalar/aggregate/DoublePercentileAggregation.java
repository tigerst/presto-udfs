package com.presto.udfs.scalar.aggregate;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.aggregate.PercentileCommon;
import com.presto.udfs.aggregate.PercentileState;

import java.util.*;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("percentile_v2")
public final class DoublePercentileAggregation {

    @InputFunction
    public static void input(@AggregationState PercentileState state,
                             @SqlType(StandardTypes.DOUBLE) double value,
                             @SqlType(StandardTypes.DOUBLE) double percentile) {
        LongPercentileAggregation.input(state, PercentileCommon.doubleToSortableLong(value), percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState PercentileState state,
                               PercentileState otherState) {
        LongPercentileAggregation.combine(state, otherState);
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState PercentileState state, BlockBuilder out) {
        double percentile = state.getPercentile();
        Map<Long, Long> counts = state.getCounts();
        if (counts == null) {
            out.appendNull();
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
            double position = maxPosition * percentile;
            DOUBLE.writeDouble(out, PercentileCommon.sortableLongToDouble(Double.valueOf(PercentileCommon.getPercentile(entriesList, position)).longValue()));
        }
    }


}
