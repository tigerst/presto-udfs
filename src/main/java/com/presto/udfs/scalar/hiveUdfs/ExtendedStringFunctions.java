package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;

import static io.airlift.slice.SliceUtf8.countCodePoints;


public class ExtendedStringFunctions
{
    private ExtendedStringFunctions() {}

    @Description("returns index of first occurrence of a substring (or 0 if not found)")
    @ScalarFunction("locate")
    @SqlType(StandardTypes.BIGINT)
    public static long locateString(@SqlType(StandardTypes.VARCHAR) Slice substring, @SqlType(StandardTypes.VARCHAR) Slice string)
    {
        if (substring.length() == 0) {
            return 1;
        }
        if (string.length() == 0) {
            return 0;
        }

        int index = string.indexOf(substring);
        return index < 0 ? 0L :(long)(SliceUtf8.countCodePoints(string, 0, index) + 1);
    }

    @Description("Returns the position of the first occurrence of substring in str after position pos")
    @ScalarFunction("locate")
    @SqlType(StandardTypes.BIGINT)
    public static long locateStringWithPos(@SqlType(StandardTypes.VARCHAR) Slice substring, @SqlType(StandardTypes.VARCHAR) Slice inputString, @SqlType(StandardTypes.BIGINT) long pos)
    {
        if (substring.length() == 0) {
            if (inputString.length() + 1 >= pos) {
               return 1;
            }
            else {
                return 0;
            }
        }
        if (inputString.length() == 0 || inputString.length() < pos) {
            return 0;
        }
        Slice string = substr(inputString, pos);
        int index = string.indexOf(substring);
        if (index < 0) {
            return 0;
        }
        return pos + countCodePoints(string, 0, index);
    }

    public static Slice substr( Slice utf8, long start) {
        if(start != 0L && utf8.length() != 0) {
            int startCodePoint = Ints.saturatedCast(start);
            int codePoints;
            int indexStart;
            if(startCodePoint > 0) {
                codePoints = SliceUtf8.offsetOfCodePoint(utf8, startCodePoint - 1);
                if(codePoints < 0) {
                    return Slices.EMPTY_SLICE;
                } else {
                    indexStart = utf8.length();
                    return utf8.slice(codePoints, indexStart - codePoints);
                }
            } else {
                codePoints = SliceUtf8.countCodePoints(utf8);
                startCodePoint += codePoints;
                if(startCodePoint < 0) {
                    return Slices.EMPTY_SLICE;
                } else {
                    indexStart = SliceUtf8.offsetOfCodePoint(utf8, startCodePoint);
                    int indexEnd = utf8.length();
                    return utf8.slice(indexStart, indexEnd - indexStart);
                }
            }
        } else {
            return Slices.EMPTY_SLICE;
        }
    }

    @Description("Returns the first occurance of string in string list (inputStrList) where string list (inputStrList) is a comma-delimited string.")
    @ScalarFunction("find_in_set")
    @SqlType(StandardTypes.BIGINT)
    public static long findInSet(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice inputStrList)
    {
        if (string.length() == 0 && inputStrList.length() == 0) {
            return 1;
        }
        String[] strList = (inputStrList.toStringUtf8()).split(",");
        long pos = 1;
        for (String s : strList) {
            if (s.equals(string.toStringUtf8())) {
                return pos;
            }
            pos++;
        }
        return 0;
    }

    @Description("returns index of first occurrence of a substring (or 0 if not found) in string")
    @ScalarFunction("instr")
    @SqlType(StandardTypes.BIGINT)
    public static long inString(@SqlType(StandardTypes.VARCHAR) Slice string, @SqlType(StandardTypes.VARCHAR) Slice substring)
    {
        if (substring.length() == 0) {
            return 1;
        }
        if (string.length() == 0) {
            return 0;
        }
        int index = string.indexOf(substring);
        return index < 0 ? 0L : (long)(SliceUtf8.countCodePoints(string, 0, index) + 1);
    }
}