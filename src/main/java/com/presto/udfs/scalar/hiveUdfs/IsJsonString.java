package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.utils.JsonUtil;
import io.airlift.slice.Slice;

@ScalarFunction("is_json_str")
public class IsJsonString {

    private IsJsonString(){

    }

    @Description("is json string")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isJsonString(@SqlType(StandardTypes.VARCHAR) Slice string) {
        return JsonUtil.isJSONString(string.toStringUtf8());
    }
}
