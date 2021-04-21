package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;

public final  class SizeFunction {

    private SizeFunction(){

    }

    @Description("Returns the cardinality (length) of the array")
    @ScalarFunction("size")
    @TypeParameter("E")
    @SqlType(StandardTypes.BIGINT)
    public static long size_array(@SqlType("array(E)") Block block)
    {
        return block.getPositionCount();
    }


    @Description("Returns the cardinality (length) of the array")
    @ScalarFunction("size")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BIGINT)
    public static long size_map(@SqlType("map(K,V)") Block block)
    {
        return block.getPositionCount() / 2;
    }

}
