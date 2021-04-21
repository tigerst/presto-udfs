package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.presto.udfs.utils.GzipUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.charset.StandardCharsets;

public class CompressFunction {

    private CompressFunction(){

    }


    @ScalarFunction("compress")
    @Description("gizp压缩base64加密，charset为ISO_8859_1")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice compress(@SqlType(StandardTypes.VARCHAR) Slice slice){
        if (slice == null) {
            return null;
        }
        return Slices.copiedBuffer(GzipUtils.compressB64Msg(slice.toString(StandardCharsets.ISO_8859_1)), StandardCharsets.ISO_8859_1);
    }

    @ScalarFunction("uncompress")
    @Description("gizp压缩base64解密，charset为ISO_8859_1")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice uncompress(@SqlType(StandardTypes.VARCHAR) Slice slice){
        if (slice == null) {
            return null;
        }
        return Slices.copiedBuffer(GzipUtils.uncompressB64Msg(slice.toString(StandardCharsets.ISO_8859_1)), StandardCharsets.ISO_8859_1);
    }

}
