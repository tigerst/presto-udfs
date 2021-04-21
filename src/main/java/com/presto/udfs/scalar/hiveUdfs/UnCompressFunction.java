package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.jdbc.internal.io.airlift.slice.Slice;
import com.facebook.presto.jdbc.internal.io.airlift.slice.Slices;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.presto.udfs.utils.GzipUtils;

public class UnCompressFunction {

    private UnCompressFunction(){

    }

    @ScalarFunction("gzip_b64_uncompress")
    @Description("gizp压缩base64解密")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice compress(@SqlType(StandardTypes.VARCHAR) Slice slice){
        if (slice == null) {
            return null;
        }
        return Slices.utf8Slice(GzipUtils.uncompressB64Msg(slice.toStringUtf8()));
    }

}
