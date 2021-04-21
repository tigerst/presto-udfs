package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.utils.SecurityUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class Base64Encode {

    private Base64Encode(){

    }

    @ScalarFunction("b64_encode")
    @Description("base64加密")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice mnc2country(@SqlType(StandardTypes.VARCHAR) Slice slice){
        if (slice == null) {
            return null;
        }
        return Slices.utf8Slice(SecurityUtils.b64encode(slice.toStringUtf8()));
    }
}
