package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.common.type.Type;

@ScalarFunction("array")
@Description("Remove duplicate values from the given array")
public class ArrayFunction {

    private ArrayFunction(){

    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block array(@TypeParameter("E") Type type, @SqlType("array(E)") Block array)
    {
        return array;
    }

}
