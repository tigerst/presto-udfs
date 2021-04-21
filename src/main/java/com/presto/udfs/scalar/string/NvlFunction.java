package com.presto.udfs.scalar.string;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;

@ScalarFunction("nvl")
public class NvlFunction {

    private NvlFunction(){

    }

    @Description("nvl(value,default_value) - Returns default value if value is null else returns value")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice nvl(@SqlType(StandardTypes.VARCHAR) Slice value, @SqlType(StandardTypes.VARCHAR) Slice default_value) {
        if(value == null){
           return default_value;
        }
        //return Slices.utf8Slice("|"+value.length()+"|");
        return value;
    }
}
