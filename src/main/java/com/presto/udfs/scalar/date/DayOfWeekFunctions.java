package com.presto.udfs.scalar.date;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Calendar;

import static java.util.concurrent.TimeUnit.DAYS;


public class DayOfWeekFunctions {

    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    private DayOfWeekFunctions(){

    }

    @ScalarFunction("dayOfWeek")
    @Description("Returns the day of week from a date string(yyyy-MM-dd)")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeek(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return -1;
        }

        try {
            LocalDate localDate = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
            return localDate.getDayOfWeek();
        } catch (Exception e) {
            return -1;
        }
    }

    @ScalarFunction("dayOfWeek")
    @Description("Returns the day of week from a date string")
    @SqlType(StandardTypes.BIGINT)
    public static long dayOfWeek(@SqlType(StandardTypes.DATE) long date) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(DAYS.toMillis(date));
            LocalDate localDate = LocalDate.fromCalendarFields(calendar);
            return localDate.getDayOfWeek();
        } catch (Exception e) {
            return -1;
        }
    }
}
