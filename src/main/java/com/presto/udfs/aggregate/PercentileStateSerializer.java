package com.presto.udfs.aggregate;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Map;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class PercentileStateSerializer
        implements AccumulatorStateSerializer<PercentileState> {

    @Override
    public Type getSerializedType() {
        return VARBINARY;
    }

    @Override
    public void serialize(PercentileState state, BlockBuilder out) {
        if (state.getCounts() == null) {
            out.appendNull();
        } else {
            Map<Long, Long> counts = state.getCounts();
            double percentile = state.getPercentile();
            SliceOutput output = Slices.allocate(
                    SIZE_OF_INT + // number of counts
                            counts.size() * 2 * SIZE_OF_LONG + // counts
                            SIZE_OF_DOUBLE // percentile
            ).getOutput();

            output.appendInt(counts.size());
            counts.entrySet().stream().forEach(entry -> {
                output.appendLong(entry.getKey());
                output.appendLong(entry.getValue());
            });
            output.appendDouble(percentile);
            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, PercentileState state) {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();
        int numCounts = input.readInt();
        ImmutableMap.Builder<Long, Long> countsBuilder = ImmutableMap.builder();
        for (int i = 0; i < numCounts; i++) {
            countsBuilder.put(input.readLong(), input.readLong());
        }
        state.setCounts(countsBuilder.build());
        state.addMemoryUsage(state.getCounts().size() * 2 * SIZE_OF_LONG);
        state.setPercentile(input.readDouble());
    }
}
