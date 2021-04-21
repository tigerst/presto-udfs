package com.presto.udfs.aggregate;

import com.facebook.presto.array.DoubleBigArray;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.SizeOf;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;


public class PercentileArrayStateFactory implements AccumulatorStateFactory<PercentileArrayState> {

    @Override
    public PercentileArrayState createSingleState() {
        return new SinglePercentileArrayState();
    }

    @Override
    public Class<? extends PercentileArrayState> getSingleStateClass() {
        return SinglePercentileArrayState.class;
    }

    @Override
    public PercentileArrayState createGroupedState() {
        return new GroupedPercentileArrayState();
    }

    @Override
    public Class<? extends PercentileArrayState> getGroupedStateClass() {
        return GroupedPercentileArrayState.class;
    }

    public static class GroupedPercentileArrayState
            extends AbstractGroupedAccumulatorState
            implements PercentileArrayState{

        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedPercentileArrayState.class).instanceSize();
        private final ObjectBigArray<Map<Long, Long>> countss = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size) {
            countss.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
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
        public List<Double> getPercentiles() {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles) {
            requireNonNull(percentiles, "value is null");
            percentilesArray.set(getGroupId(), percentiles);
        }

        @Override
        public void addMemoryUsage(int value) {
            size += value;
        }

        @Override
        public long getEstimatedSize() {
            return INSTANCE_SIZE  + size + countss.sizeOf() + percentilesArray.sizeOf();
        }
    }

    public static class SinglePercentileArrayState implements PercentileArrayState{

        public static final int INSTANCE_SIZE = ClassLayout.parseClass(PercentileArrayState.class).instanceSize();
        private Map<Long, Long> counts;
        private List<Double> percentiles;

        @Override
        public Map<Long, Long> getCounts() {
            return counts;
        }

        @Override
        public void setCounts(Map<Long, Long> counts) {
            this.counts = requireNonNull(counts, "counts is null");
        }

        @Override
        public List<Double> getPercentiles() {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles) {
            this.percentiles = percentiles;
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
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }
    }
}
