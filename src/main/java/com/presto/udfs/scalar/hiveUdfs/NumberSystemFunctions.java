package com.presto.udfs.scalar.hiveUdfs;


import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import static java.nio.charset.StandardCharsets.UTF_8;

public class NumberSystemFunctions
{
    private NumberSystemFunctions(){

    }

    @Description("Converts decimal number to binary")
    @ScalarFunction("bin_eleme")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice bin(@SqlType(StandardTypes.BIGINT) long num)
    {
        String bin = Long.toBinaryString(num);
        return Slices.copiedBuffer(bin, UTF_8);
    }

    @ScalarFunction("sbin")
    @Description("Converts  number ")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice sbin(@SqlType(StandardTypes.DOUBLE) double num)
    {
        String sbin = Double.toHexString(num);
        return Slices.copiedBuffer(sbin, UTF_8);
    }

    @ScalarFunction("is_null")
    @Description("Returns TRUE if the argument is NULL")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean isnull(@Nullable @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        return (string == null);
    }

    @ScalarFunction("unixtime")
    @SqlType(StandardTypes.BIGINT)
    public static long UnixTime(@SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return timestamp / 1000;
    }

    public static final String PROP_MD5_PREFIX = "ele_isaiah_20141111";

    /*@ScalarFunction("md5")
    @Description("_FUNC_(message) - return the md5 checksum for the message.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice md5(@SqlType(StandardTypes.VARCHAR) Slice str)
    {
        String result = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");

            byte[] theDigest = md.digest((PROP_MD5_PREFIX + str.toString(Charsets.UTF_8))
                    .getBytes("UTF-8"));

            for (int i = 0; i < theDigest.length; i++) {
                result += Integer.toString((theDigest[i] & 0xff) + 0x100, 16).substring(1);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return Slices.copiedBuffer(result, Charsets.UTF_8);
    }*/
}