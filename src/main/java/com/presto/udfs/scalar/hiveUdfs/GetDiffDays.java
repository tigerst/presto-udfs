package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.tools.DateUtils;
import io.airlift.slice.Slice;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

public class GetDiffDays {

    private GetDiffDays(){

    }

    @ScalarFunction("get_diff_days")
    @Description("获取起始日期（格式为20191231）到结束日期之间的间隔天数（起始日期 == 结束日期 则 返回 1），异常返回-1")
    @SqlType(StandardTypes.BIGINT)
    public static long getDiffDays(@SqlType(StandardTypes.VARCHAR) Slice startDateSlice, @SqlType(StandardTypes.VARCHAR) Slice endDateSlice) {
        if (startDateSlice == null || endDateSlice == null) {
            return -1;
        }

        String startDate = startDateSlice.toStringUtf8();
        String endDate = endDateSlice.toStringUtf8();

        if (startDate.length() != startDate.length() || startDate.length() != 8) {
            return -1;
        }

        long betweenVal = DateUtils.getBetweenVal(startDate, endDate, ChronoUnit.DAYS);

        return betweenVal + 1;
    }

    @ScalarFunction("get_diff_days")
    @Description("获取起始日期（格式为20191231）到结束日期之间的间隔unit（y为年，m为月，m都天，默认为天）数（起始日期 == 结束日期 则 返回 1），异常返回-1")
    @SqlType(StandardTypes.INTEGER)
    public static long getDiffDays(@SqlType(StandardTypes.VARCHAR) Slice startDateSlice,
                                   @SqlType(StandardTypes.VARCHAR) Slice endDateSlice,
                                   @SqlType(StandardTypes.VARCHAR) Slice unitSlice) {
        if (startDateSlice == null || endDateSlice == null || unitSlice == null) {
            return -1;
        }

        String startDate = startDateSlice.toStringUtf8();
        String endDate = endDateSlice.toStringUtf8();
        String unit = unitSlice.toStringUtf8();

        if (startDate.length() != startDate.length() || startDate.length() != 8) {
            return -1;
        }

        unit = unit.toLowerCase();
        TemporalUnit temporalUnit = ChronoUnit.DAYS;
        if (unit.startsWith("y")) {
            temporalUnit = ChronoUnit.YEARS;
        } else if (unit.startsWith("m")) {
            temporalUnit = ChronoUnit.MONTHS;
        }

        long betweenVal = DateUtils.getBetweenVal(startDate, endDate, temporalUnit);

        return betweenVal + 1;
    }
}
