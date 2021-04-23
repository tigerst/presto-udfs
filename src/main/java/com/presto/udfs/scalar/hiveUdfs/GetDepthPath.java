package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.tools.UDFConstant;
import com.presto.udfs.utils.ConfigUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.stream.Collectors;

public class GetDepthPath {

    private GetDepthPath(){

    }

    @Description("Returns the path in depth split by prefix")
    @ScalarFunction("get_depth_path")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getDepthPath(@SqlType(StandardTypes.VARCHAR) Slice pathSlice,
                                 @SqlType(StandardTypes.INTEGER) long depth){
        return getDepthPath(Slices.utf8Slice(ConfigUtils.getInstance().getHdfsPrePath()), pathSlice, depth);
    }

    @Description("Returns the path in depth split by prefix")
    @ScalarFunction("get_depth_path")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getDepthPath(@SqlType(StandardTypes.VARCHAR) Slice prefixSlice,
                                 @SqlType(StandardTypes.VARCHAR) Slice pathSlice,
                                 @SqlType(StandardTypes.INTEGER) long depth){
        String prefix = prefixSlice.toStringUtf8().toLowerCase();
        String path = pathSlice.toStringUtf8().toLowerCase();
        prefix = prefix.trim();
        path = path.trim();

        if (depth < 0) {
            return null;
        }

        if (path.indexOf(prefix) < 0) {
            return null;
        }
        String substring = path.substring(prefix.length());
        String[] split = substring.split("/");
        long start = prefix.isEmpty() ? depth + 1 : depth;
        return Slices.utf8Slice(prefix + Arrays.stream(split).limit(start).collect(Collectors.joining("/")));
    }
}
