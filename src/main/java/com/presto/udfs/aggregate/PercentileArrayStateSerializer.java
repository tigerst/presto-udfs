package com.presto.udfs.aggregate;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.*;

public class PercentileArrayStateSerializer
        implements AccumulatorStateSerializer<PercentileArrayState> {

    @Override
    public Type getSerializedType() {
        return VARBINARY;
    }

    @Override
    public void serialize(PercentileArrayState state, BlockBuilder out) {
        if (state.getCounts() == null) {
            out.appendNull();
        } else {
            Map<Long, Long> counts = state.getCounts();
            List<Double> percentiles = state.getPercentiles();
            SliceOutput output = Slices.allocate(
                    SIZE_OF_INT + // number of counts
                            counts.size() * 2 * SIZE_OF_LONG + // counts
                            SIZE_OF_INT + // number of percentile
                            percentiles.size() * SIZE_OF_DOUBLE // percentile
            ).getOutput();

            output.appendInt(counts.size());
            counts.entrySet().stream().forEach(entry -> {
                output.appendLong(entry.getKey());
                output.appendLong(entry.getValue());
            });
            output.appendInt(percentiles.size());
            percentiles.stream().forEach(percentile -> {
                output.appendDouble(percentile);
            });
            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, PercentileArrayState state) {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        int numCounts = input.readInt();
        ImmutableMap.Builder<Long, Long> countsBuilder = ImmutableMap.builder();
        for (int i = 0; i < numCounts; i++) {
            countsBuilder.put(input.readLong(), input.readLong());
        }
        state.setCounts(countsBuilder.build());

        int percentileNum = input.readInt();
        ImmutableList.Builder<Double> percentileBuilder = ImmutableList.builder();
        for (int i = 0; i < percentileNum; i++) {
            percentileBuilder.add(input.readDouble());
        }
        state.setPercentiles(percentileBuilder.build());

    }
}
