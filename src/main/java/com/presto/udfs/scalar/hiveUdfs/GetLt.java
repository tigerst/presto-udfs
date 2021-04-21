package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;

public class GetLt {

    private GetLt(){

    }

    @Description("get lt")
    @ScalarFunction("get_lt")
    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType(StandardTypes.BIGINT)
    public static long getLt(@TypeParameter("K") Type keyType,
                             @TypeParameter("V") Type valueType,
                             @SqlType(StandardTypes.BIGINT) long left,
                             @SqlType(StandardTypes.BIGINT) long right,
                             @SqlType("map(K,V)") Block block) {
        if (left > right) {
            return 0;
        }

        long total = 0;
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            long key = Long.valueOf(keyType.getSlice(block, i).toStringUtf8());
            long val = Long.valueOf(valueType.getSlice(block, i + 1).toStringUtf8());
            if (key >= left && key <= right) {
                total += val;
            }
        }

        return total;
    }

}
