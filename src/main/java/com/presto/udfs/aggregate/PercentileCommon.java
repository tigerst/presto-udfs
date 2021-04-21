package com.presto.udfs.aggregate;

import io.airlift.slice.SizeOf;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PercentileCommon {

    /**
     * A comparator to sort the entries in order.
     */
    public static class MyComparator implements Comparator<Map.Entry<Long, Long>> {
        @Override
        public int compare(Map.Entry<Long, Long> o1,
                           Map.Entry<Long, Long> o2) {
            return (int) (o1.getKey() - o2.getKey());
        }
    }

    public static int getSizeInBytes(Map<Long, Long> counts) {
        return 2 * counts.size() * SizeOf.SIZE_OF_LONG;
    }

    /**
     * Increment the State object with o as the key, and i as the count.
     */
    public static void increment(PercentileState s, Long o, long i) {
        if (s.getCounts() == null) {
            Map<Long, Long> counts = new HashMap<>();
            s.setCounts(counts);
            s.addMemoryUsage(getSizeInBytes(counts));
        }

        s.addMemoryUsage(-getSizeInBytes(s.getCounts()));
        Long count = s.getCounts().get(o);
        if (count == null) {
            s.getCounts().put(o, i);
        } else {
            s.getCounts().put(o, count + i);
        }
        s.addMemoryUsage(getSizeInBytes(s.getCounts()));
    }

    /**
     * Increment the State object with o as the key, and i as the count.
     */
    public static void incrementArray(PercentileArrayState s, Long o, long i) {
        if (s.getCounts() == null) {
            Map<Long, Long> counts = new HashMap<>();
            s.setCounts(counts);
            s.addMemoryUsage(getSizeInBytes(counts));
        }

        s.addMemoryUsage(-getSizeInBytes(s.getCounts()));
        Long count = s.getCounts().get(o);
        if (count == null) {
            s.getCounts().put(o, i);
        } else {
            s.getCounts().put(o, count + i);
        }
        s.addMemoryUsage(getSizeInBytes(s.getCounts()));
    }

    /**
     * Get the percentile value.
     */
    public static double getPercentile(List<Map.Entry<Long, Long>> entriesList,
                                        double position) {
        // We may need to do linear interpolation to get the exact percentile
        long lower = (long) Math.floor(position);
        long higher = (long) Math.ceil(position);

        // Linear search since this won't take much time from the total execution anyway
        // lower has the range of [0 .. total-1]
        // The first entry with accumulated count (lower+1) corresponds to the lower position.
        int i = 0;
        while (entriesList.get(i).getValue() < lower + 1) {
            i++;
        }

        long lowerKey = entriesList.get(i).getKey();
        if (higher == lower) {
            // no interpolation needed because position does not have a fraction
            return lowerKey;
        }

        if (entriesList.get(i).getValue() < higher + 1) {
            i++;
        }
        long higherKey = entriesList.get(i).getKey();

        if (higherKey == lowerKey) {
            // no interpolation needed because lower position and higher position has the same key
            return lowerKey;
        }

        // Linear interpolation to get the exact percentile
        return (higher - position) * lowerKey + (position - lower) * higherKey;
    }

    /**
     * Converts a double value to a sortable long. The value is converted by getting their IEEE 754
     * floating-point bit layout. Some bits are swapped to be able to compare the result as long.
     */
    public static long doubleToSortableLong(double value)
    {
        long bits = Double.doubleToLongBits(value);
        return bits ^ (bits >> 63) & Long.MAX_VALUE;
    }

    /**
     * Converts a sortable long to double.
     *
     * @see #sortableLongToDouble(long)
     */
    public static double sortableLongToDouble(long value)
    {
        value = value ^ (value >> 63) & Long.MAX_VALUE;
        return Double.longBitsToDouble(value);
    }
}
