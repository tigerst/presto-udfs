package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.StringUtils;

public class SubStringIndexFunction {

    private SubStringIndexFunction(){

    }

    @ScalarFunction(value = "substring_index")//str, delim, count
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice geohash( @SqlType(StandardTypes.VARCHAR)  Slice str,
                                 @SqlType(StandardTypes.VARCHAR)  Slice delim,
                                 @SqlType(StandardTypes.BIGINT)  long count
                                 )
    {
        String res;
        if (count > 0) {
            int idx = StringUtils.ordinalIndexOf(str.toStringUtf8(), delim.toStringUtf8(), (int)count);
            if (idx != -1) {
                res = str.toStringUtf8().substring(0, idx);
            } else {
                res = str.toStringUtf8();
            }
        } else {
            int idx = StringUtils.lastOrdinalIndexOf(str.toStringUtf8(), delim.toStringUtf8(), -(int)count);
            if (idx != -1) {
                res = str.toStringUtf8().substring(idx + 1);
            } else {
                res = str.toStringUtf8();
            }
        }
        return Slices.utf8Slice(res);
    }


}
