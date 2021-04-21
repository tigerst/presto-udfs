package com.presto.udfs.aggregate;

import com.facebook.presto.spi.function.GroupedAccumulatorState;

public abstract class AbstractGroupedAccumulatorState implements GroupedAccumulatorState {
    private long groupId;

    @Override
    public final void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }

    protected final long getGroupId()
    {
        return groupId;
    }
}
