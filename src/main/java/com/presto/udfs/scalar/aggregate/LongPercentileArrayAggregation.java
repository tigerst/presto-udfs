package com.presto.udfs.scalar.aggregate;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import com.presto.udfs.aggregate.PercentileArrayState;
import com.presto.udfs.aggregate.PercentileCommon;

import java.util.*;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

@AggregationFunction("percentile_v2")
public final class LongPercentileArrayAggregation {

    @InputFunction
    public static void input(@AggregationState PercentileArrayState state,
                             @SqlType(StandardTypes.BIGINT) long value,
                             @SqlType("array(double)") Block percentilesArrayBlock) {
        initializePercentilesArray(state, percentilesArrayBlock);
        PercentileCommon.incrementArray(state, value, 1);
    }

    @CombineFunction
    public static void combine(@AggregationState PercentileArrayState state,
                               PercentileArrayState otherState) {
        Map<Long, Long> input = otherState.getCounts();
        Map<Long, Long> previous = state.getCounts();
        if (previous == null) {
            state.setCounts(input);
            state.addMemoryUsage(PercentileCommon.getSizeInBytes(input));
        } else {
            state.addMemoryUsage(-PercentileCommon.getSizeInBytes(state.getCounts()));
            input.entrySet().stream().forEach(entry -> {
                PercentileCommon.incrementArray(state, entry.getKey(), entry.getValue());
            });
            state.addMemoryUsage(PercentileCommon.getSizeInBytes(state.getCounts()));
        }
        state.setPercentiles(otherState.getPercentiles());
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
                DOUBLE.writeDouble(blockBuilder, PercentileCommon.getPercentile(entriesList, position));
            }
            out.closeEntry();

        }
    }

    private static void initializePercentilesArray(@AggregationState PercentileArrayState state,
                                                   Block percentilesArrayBlock) {
        if (state.getPercentiles() == null) {
            ImmutableList.Builder<Double> percentilesListBuilder = ImmutableList.builder();

            for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
                if (percentilesArrayBlock.isNull(i)) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentile cannot be null");
                }
                double percentile = DOUBLE.getDouble(percentilesArrayBlock, i);
                if (!(0 <= percentile && percentile <= 1)) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Percentile must be between 0 and 1");
                }
                percentilesListBuilder.add(percentile);
            }

            state.setPercentiles(percentilesListBuilder.build());
        }
    }
}
