package com.presto.udfs.scalar.string;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;

@ScalarFunction("str_check")
public class StringCheck {

    private StringCheck(){

    }

    @Description("标准字符串检测，正则为^[0-9a-zA-Z\\u4e00-\\u9fa5][0-9a-zA-Z\\uff08\\uff09\\u4e00-\\u9fa5/\\-\\_\\s\\+\\.\\:\\~\\&\\*]+$")
    @SqlType(StandardTypes.BIGINT)
    public static long stringCheck(@SqlType(StandardTypes.VARCHAR) Slice strSlice) {
        if (strSlice == null) {
            return 1;
        }
        String str = strSlice.toStringUtf8();
        boolean matches = str.matches("^[0-9a-zA-Z\\u4e00-\\u9fa5][0-9a-zA-Z\\uff08\\uff09\\u4e00-\\u9fa5/\\-\\_\\s\\+\\.\\:\\~\\&\\*]+$");
        return matches ? 1 : 0;
    }
}
