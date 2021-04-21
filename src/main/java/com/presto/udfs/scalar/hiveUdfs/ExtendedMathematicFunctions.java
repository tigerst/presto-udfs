package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ExtendedMathematicFunctions
{
    private ExtendedMathematicFunctions() {}

    @Description("a seed pseudo-random value")
    @ScalarFunction("rands")
    @SqlType(StandardTypes.DOUBLE)
    public static double randomWithSeed(@SqlType(StandardTypes.BIGINT) long seed)
    {
        Random rand = new Random(seed);
        return rand.nextDouble();
    }


    @SqlNullable
    @Description("Returns the positive value of a mod b.")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.BIGINT)
    public static Long positiveModulus(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
    {
        if (b == 0) {
            return null;
        }
        return Math.floorMod(a, b);
    }

    @SqlNullable
    @Description("Returns the positive value of a mod b ")
    @ScalarFunction("pmod")
    @SqlType(StandardTypes.DOUBLE)
    public static Double positiveModulusDouble(@SqlType(StandardTypes.DOUBLE) double a, @SqlType(StandardTypes.DOUBLE) double b)
    {
        if (b == 0) {
            return null;
        }

        if ((a >= 0 && b > 0) || (a <= 0 && b < 0)) {
            return a % b;
        }
        else {
            return (a % b) + b;
        }
    }

    @SqlNullable
    @Description("Converts decimal number to binary")
    @ScalarFunction("bin")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice integerToBin(@SqlType(StandardTypes.BIGINT) long num)
    {
        String bin = Long.toBinaryString(num);
        return Slices.copiedBuffer(bin, UTF_8);
    }


    @SqlNullable
    @Description("Converts integer number to hex value")
    @ScalarFunction("hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice integerToHex(@SqlType(StandardTypes.BIGINT) long num)
    {
        String hex = Long.toHexString(num);
        return Slices.copiedBuffer(hex, UTF_8);
    }


    @SqlNullable
    @Description("Converts string number to hex value")
    @ScalarFunction("hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringIntegerToHex(@SqlType(StandardTypes.VARCHAR) Slice inputNum)
    {
        try {
            Long num = Long.parseLong(inputNum.toStringUtf8(), 10);
            String hex = Long.toHexString(num);
            return Slices.copiedBuffer(hex, UTF_8);
        }
        catch (NumberFormatException e) {
            // Return NULL if invalid input
            return null;
        }
    }
    @SqlNullable
    @Description("Converts binary to hex value")
    @ScalarFunction("hex")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice BinaryrToHex(@SqlType(StandardTypes.VARBINARY) Slice inputNum)
    {
        try {
            Long num = Long.parseLong(inputNum.toStringUtf8(), 2);
            String hex = Long.toHexString(num);
            return Slices.copiedBuffer(hex, UTF_8);
        }
        catch (NumberFormatException e) {
            // Return NULL if invalid input
            return null;
        }
    }

    @SqlNullable
    @Description("Converts Hexadecimal number to binary value")
    @ScalarFunction("unhex")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice hexToInteger(@SqlType(StandardTypes.VARCHAR) Slice inputHex)
    {
        try {
            Long num = Long.parseLong(inputHex.toStringUtf8(), 16);
            return Slices.copiedBuffer(Long.toBinaryString(num), UTF_8);
        }
        catch (NumberFormatException e) {
            // Return NULL if invalid input
            return null;
        }
    }
}