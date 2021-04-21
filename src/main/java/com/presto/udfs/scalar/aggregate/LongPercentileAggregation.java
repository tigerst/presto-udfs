package com.presto.udfs.scalar.aggregate;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.aggregate.PercentileCommon;
import com.presto.udfs.aggregate.PercentileState;

import java.util.*;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("percentile_v2")
public final class LongPercentileAggregation {

    @InputFunction
    public static void input(@AggregationState PercentileState state,
                             @SqlType(StandardTypes.BIGINT) long value,
                             @SqlType(StandardTypes.DOUBLE) double percentile) {
        if (!(0 <= percentile && percentile <= 1)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT ,"Percentile must be between 0 and 1");
        }

        PercentileCommon.increment(state, value, 1);
        state.setPercentile(percentile);
    }

    @CombineFunction
    public static void combine(@AggregationState PercentileState state,
                               PercentileState otherState) {
        Map<Long, Long> input = otherState.getCounts();
        Map<Long, Long> previous = state.getCounts();
        if (previous == null) {
            state.setCounts(input);
            state.addMemoryUsage(PercentileCommon.getSizeInBytes(input));
        } else {
            state.addMemoryUsage(-PercentileCommon.getSizeInBytes(state.getCounts()));
            input.entrySet().stream().forEach(entry -> {
                PercentileCommon.increment(state, entry.getKey(), entry.getValue());
            });
            state.addMemoryUsage(PercentileCommon.getSizeInBytes(state.getCounts()));
        }
        state.setPercentile(otherState.getPercentile());
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
            DOUBLE.writeDouble(out, PercentileCommon.getPercentile(entriesList, position));

        }
    }


}
