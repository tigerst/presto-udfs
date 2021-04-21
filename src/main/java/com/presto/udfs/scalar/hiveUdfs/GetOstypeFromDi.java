package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class GetOstypeFromDi {

    private GetOstypeFromDi() {

    }

    @ScalarFunction("get_ostype_from_di")
    @Description("")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getOstypeFromDi(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        if (slice == null) {
            return Slices.utf8Slice("android");
        }

        String val = slice.toStringUtf8();
        val = val.toLowerCase();
        if (val.contains("iphone") || val.contains("ipad") || val.contains("ipod")) {
            return Slices.utf8Slice("ios");
        }

        return Slices.utf8Slice("android");
    }

}
