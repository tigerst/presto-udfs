package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.*;
import com.google.common.base.Charsets;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public class ConcatWsFunction {

    private ConcatWsFunction(){

    }

    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组，数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }

        int length = block.getPositionCount();

        if (length <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
                    "The function CONCAT_WS(separator, array(string)) "
                            + "needs at least two arguments.");
        }

        //varchar
        VarcharType varcharType = VarcharType.createUnboundedVarcharType();

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < length; i++) {
            try {
                String val = varcharType.getSlice(block, i).toStringUtf8();
                if (val == null) {
                    continue;
                }
                sb.append(val).append(separator);
            } catch (Exception e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Argument " + (i + 1)
                        + " of function CONCAT_WS must be \"string"
                        + " or array<string>\".");
            }
        }
        //移除最后的分隔符
        return Slices.copiedBuffer(sb.substring(0, sb.length() - separator.length()), Charsets.UTF_8);
    }


    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2);
    }

    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3);
    }


    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4);
    }

    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4, @SqlType("array(varchar)") Block block5) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4, block5);
    }

    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4, @SqlType("array(varchar)") Block block5,
                                 @SqlType("array(varchar)") Block block6) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4, block5, block6);
    }

    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4, @SqlType("array(varchar)") Block block5,
                                 @SqlType("array(varchar)") Block block6, @SqlType("array(varchar)") Block block7) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4, block5, block6, block7);
    }


    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4, @SqlType("array(varchar)") Block block5,
                                 @SqlType("array(varchar)") Block block6, @SqlType("array(varchar)") Block block7, @SqlType("array(varchar)") Block block8) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4, block5, block6, block7, block8);
    }

    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4, @SqlType("array(varchar)") Block block5,
                                 @SqlType("array(varchar)") Block block6, @SqlType("array(varchar)") Block block7, @SqlType("array(varchar)") Block block8,
                                 @SqlType("array(varchar)") Block block9) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4, block5, block6, block7, block8, block9);
    }



    @ScalarFunction("concat_ws")
    @Description("带有分隔符的字符串拼接, 参数传入数组, 数组个数小于等于10")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice concatWs(@SqlType(StandardTypes.VARCHAR) Slice separatorSlice, @SqlType("array(varchar)") Block block1, @SqlType("array(varchar)") Block block2,
                                 @SqlType("array(varchar)") Block block3, @SqlType("array(varchar)") Block block4, @SqlType("array(varchar)") Block block5,
                                 @SqlType("array(varchar)") Block block6, @SqlType("array(varchar)") Block block7, @SqlType("array(varchar)") Block block8,
                                 @SqlType("array(varchar)") Block block9, @SqlType("array(varchar)") Block block10) {
        String separator = separatorSlice.toStringUtf8();
        if (separator == null) {
            return null;
        }
        return concatWs(separator, block1, block2, block3, block4, block5, block6, block7, block8, block9, block10);
    }


    /**
     * 可变参数
     * @param separator
     * @param blocks
     * @return
     */
    private static Slice concatWs(String separator, Block... blocks){
        //varchar
        VarcharType varcharType = VarcharType.createUnboundedVarcharType();
        int length = blocks.length;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            Block element = blocks[i];
            if (element == null || element.isNull(i)) {
                //为null的跳过
                continue;
            }

            int subElementLength = element.getPositionCount();
            for (int j = 0; j < subElementLength; j++) {
                try {
                    String val = varcharType.getSlice(element, j).toStringUtf8();
                    if (val == null) {
                        continue;
                    }
                    sb.append(val).append(separator);
                } catch (Exception e) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Argument " + (i + 1)
                            + " of function CONCAT_WS must be \"array<string>\".");
                }
            }
        }
        //移除最后的分隔符
        return Slices.copiedBuffer(sb.substring(0, sb.length() - separator.length()), Charsets.UTF_8);
    }

}
