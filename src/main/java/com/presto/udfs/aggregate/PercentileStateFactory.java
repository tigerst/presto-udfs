package com.presto.udfs.aggregate;

import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.Map;

import static java.util.Objects.requireNonNull;


public class PercentileStateFactory  implements AccumulatorStateFactory<PercentileState> {

    @Override
    public PercentileState createSingleState() {
        return new SinglePercentileState();
    }

    @Override
    public Class<? extends PercentileState> getSingleStateClass() {
        return SinglePercentileState.class;
    }

    @Override
    public PercentileState createGroupedState() {
        return new GroupedPercentileState();
    }

    @Override
    public Class<? extends PercentileState> getGroupedStateClass() {
        return GroupedPercentileState.class;
    }

    public static class GroupedPercentileState
            extends AbstractGroupedAccumulatorState
            implements PercentileState{

        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedPercentileState.class).instanceSize();
        private final ObjectBigArray<Map<Long, Long>> countss = new ObjectBigArray<>();
        private final DoubleBigArray precentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size) {
            countss.ensureCapacity(size);
            precentiles.ensureCapacity(size);
        }

        @Override
        public Map<Long, Long> getCounts() {
            return countss.get(getGroupId());
        }

        @Override
        public void setCounts(Map<Long, Long> counts) {
            requireNonNull(counts, "value is null");
            countss.set(getGroupId(), counts);
        }

        @Override
        public double getPercentile() {
            return precentiles.get(getGroupId());
        }

        @Override
        public void setPercentile(double precentile) {
            precentiles.set(getGroupId(), precentile);
        }

        @Override
        public void addMemoryUsage(int value) {
            size += value;
        }

        @Override
        public long getEstimatedSize() {
            return INSTANCE_SIZE  + size + countss.sizeOf() + precentiles.sizeOf();
        }
    }

    public static class SinglePercentileState implements PercentileState{

        public static final int INSTANCE_SIZE = ClassLayout.parseClass(PercentileState.class).instanceSize();
        private Map<Long, Long> counts;
        private double precentile;

        @Override
        public Map<Long, Long> getCounts() {
            return counts;
        }

        @Override
        public void setCounts(Map<Long, Long> counts) {
            this.counts = requireNonNull(counts, "counts is null");
        }

        @Override
        public double getPercentile() {
            return precentile;
        }

        @Override
        public void setPercentile(double precentile) {
            this.precentile = precentile;
        }

        @Override
        public void addMemoryUsage(int value) {

        }

        @Override
        public long getEstimatedSize() {
            long estimatedSize = INSTANCE_SIZE;
            if (counts != null) {
                estimatedSize += 2 * counts.size() * SizeOf.SIZE_OF_LONG;
            }
            return estimatedSize;
        }
    }
}
