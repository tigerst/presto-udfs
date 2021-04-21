package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeField;
import org.joda.time.Days;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;

import static com.presto.udfs.utils.DateTimeFunctions.*;
import static com.facebook.presto.common.type.DateTimeEncoding.packDateTimeWithZone;
import static io.airlift.slice.Slices.utf8Slice;

public class ExtendedDateTimeFunctions {

    private ExtendedDateTimeFunctions() {}


    @Description("given timestamp in UTC and converts to given timezone")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUtcTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(timestamp, zoneId.toString());
        return  timestamp + ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp (in varchar) in UTC and converts to given timezone")
    @ScalarFunction("from_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long fromUtcTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        Timestamp javaTimestamp = Timestamp.valueOf(inputTimestamp.toStringUtf8());
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(javaTimestamp.getTime(), zoneId.toString());
        return  javaTimestamp.getTime() + ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp in a timezone convert it to UTC")
    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toUtcTimestamp(@SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp =  packDateTimeWithZone(timestamp, zoneId.toString());
        return timestamp - ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }

    @Description("given timestamp (in varchar) in a timezone convert it to UTC")
    @ScalarFunction("to_utc_timestamp")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toUtcTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp, @SqlType(StandardTypes.VARCHAR) Slice inputZoneId)
    {
        Timestamp javaTimestamp = Timestamp.valueOf(inputTimestamp.toStringUtf8());
        ZoneId zoneId = ZoneId.of(inputZoneId.toStringUtf8(), ZoneId.SHORT_IDS);
        long offsetTimestamp = packDateTimeWithZone(javaTimestamp.getTime(), zoneId.toString());
        return  javaTimestamp.getTime() - ((timeZoneHourFromTimestampWithTimeZone(offsetTimestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(offsetTimestamp)) * 60) * 1000;
    }



    @Description("Gets current UNIX timestamp in seconds")
    @ScalarFunction("unix_timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long currentUnixTimestamp(SqlFunctionProperties properties)
    {
        return properties.getSessionStartTime() / 1000;
    }

    @Description("Subtract number of days to the given date")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateSub(SqlFunctionProperties properties, @SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long value)
    {
        date = addFieldValueDate(Slices.utf8Slice("day"), -value, date);
        return toISO8601FromDate(date);
    }

    @Description("Subtract number of days to the given string date")
    @ScalarFunction("date_sub")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringDateSub(@SqlType(StandardTypes.VARCHAR) Slice inputDate, @SqlType(StandardTypes.BIGINT) long value)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(inputDate.toStringUtf8(), formatter);
        date = LocalDate.ofEpochDay(date.toEpochDay() - value);
        return utf8Slice(date.toString());
    }

    @Description("Add number of days to the given date")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.BIGINT) long value)
    {
        date = addFieldValueDate(Slices.utf8Slice("day"), value, date);
        return toISO8601FromDate(date);
    }

    @Description("Add number of days to the given string date")
    @ScalarFunction("date_add")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice StringDateAdd(@SqlType(StandardTypes.VARCHAR) Slice inputDate, @SqlType(StandardTypes.BIGINT) long value)
    {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date = LocalDate.parse(inputDate.toStringUtf8(), formatter);
        date = LocalDate.ofEpochDay(date.toEpochDay() + value);
        return utf8Slice(date.toString());
    }

    @Description("year of the given string timestamp")
    @ScalarFunction("year")
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        //return Year.parse(inputTimestamp.toStringUtf8(), formatter).getValue();
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getYear();
    }

    @Description("month of the year of the given string timestamp")
    @ScalarFunction("month")
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        //return MonthDay.parse(inputTimestamp.toStringUtf8(), formatter).getMonthValue();
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getMonthOfYear();
    }

    @Description("week of the year of the given string timestamp")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYearFromStringTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        //LocalDate date = LocalDate.parse(inputTimestamp.toStringUtf8(), formatter);
        //return date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);

        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getWeekOfWeekyear();
    }

    @Description("week of the year of the given string timestamp")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYearFromTimestamp(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {
        return weekFromTimestamp(properties, timestamp);
    }

    @Description("week of the year of the given string timestamp")
    @ScalarFunction("weekofyear")
    @SqlType(StandardTypes.BIGINT)
    public static long weekOfYearFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        return weekFromTimestampWithTimeZone(timestamp);
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction(value = "day", alias = "dayofmonth")
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        //return MonthDay.parse(inputTimestamp.toStringUtf8(), formatter).getDayOfMonth();
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getDayOfMonth();
    }

    public static final org.joda.time.format.DateTimeFormatter TIMESTAMP_WITH_TIME_ZONE_FORMATTER;
    static
    {
        DateTimeParser[] timestampWithTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-MM-dd").getParser(),
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").getParser(),
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S").getParser(),
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getParser(),
                DateTimeFormat.forPattern("HH:mm:ss").getParser(),
                DateTimeFormat.forPattern("HH:mm:ss.S").getParser(),
                DateTimeFormat.forPattern("HH:mm:ss.SSS").getParser()
        };
        DateTimePrinter timestampWithTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ").getPrinter();
        TIMESTAMP_WITH_TIME_ZONE_FORMATTER = new org.joda.time.format.DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();
    }


    @Description("day of the year of the given string timestamp")
    @ScalarFunction("hour")
    @SqlType(StandardTypes.BIGINT)
    public static long hourFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {

        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd ]HH:mm:ss[.SSS][ zzz]");
        //return LocalTime.parse(inputTimestamp.toStringUtf8(), formatter).getHour();
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getHourOfDay();
    }


    @Description("day of the year of the given string timestamp")
    @ScalarFunction("minute")
    @SqlType(StandardTypes.BIGINT)
    public static long minuteFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd ]HH:mm:ss[.SSS][ zzz]");
        //return LocalTime.parse(inputTimestamp.toStringUtf8(), formatter).getMinute();
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getMinuteOfHour();
    }

    @Description("day of the year of the given string timestamp")
    @ScalarFunction("second")
    @SqlType(StandardTypes.BIGINT)
    public static long secondFromTimestamp(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[yyyy-MM-dd ]HH:mm:ss[.SSS][ zzz]");
        //return LocalTime.parse(inputTimestamp.toStringUtf8(), formatter).getSecond();
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).getSecondOfMinute();
    }

    @Description("difference of the given dates (String) in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffStringDateInDays(@SqlType(StandardTypes.VARCHAR) Slice inputDate1, @SqlType(StandardTypes.VARCHAR) Slice inputDate2)
    {
        return Days.daysBetween(TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputDate2.toStringUtf8()),
                TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputDate1.toStringUtf8())).getDays();
        /**
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss[.SSS]][ zzz]");
        LocalDate date1 = LocalDate.parse(inputDate1.toStringUtf8(), formatter);
        LocalDate date2 = LocalDate.parse(inputDate2.toStringUtf8(), formatter);
        return date1.toEpochDay() - date2.toEpochDay();
         **/
    }

    @Description("difference of the given dates in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffDateInDays(@SqlType(StandardTypes.DATE) long date1, @SqlType(StandardTypes.DATE) long date2)
    {
        return diffDate(utf8Slice("day"), date2, date1);
    }

    @Description("difference of the given dates (Timestamps) in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestampDateInDays(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp1, @SqlType(StandardTypes.TIMESTAMP) long timestamp2)
    {
        return diffTimestamp(properties, utf8Slice("day"), timestamp2, timestamp1);
    }

    @Description("difference of the given dates (Timestamps) in days")
    @ScalarFunction("datediff")
    @SqlType(StandardTypes.BIGINT)
    public static long diffTimestampWithTimezoneDateInDays(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp1, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp2)
    {
        return diffTimestampWithTimeZone(utf8Slice("day"), timestamp2, timestamp1);
    }



    private static DateTimeField getDateField(ISOChronology chronology)
    {
        return chronology.dayOfMonth();
    }

    @Description("Converts the number of seconds from unix epoch to a string representing the timestamp")
    @ScalarFunction("format_unixtimestamp")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixtimeToStringTimestamp(@SqlType(StandardTypes.BIGINT) long epochtime)
    {
        LocalDateTime timestamp = LocalDateTime.ofEpochSecond(epochtime, 0, ZoneOffset.UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return utf8Slice(timestamp.format(formatter));
    }

    @Description("Converts the number of seconds from unix epoch to a string representing the timestamp according to the given format")
    @ScalarFunction("format_unixtimestamp")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice fromUnixtimeWithFormatToStringTimestamp(@SqlType(StandardTypes.BIGINT) long epochtime, @SqlType(StandardTypes.VARCHAR) Slice format)
    {
        ZonedDateTime timestamp = ZonedDateTime.of(LocalDateTime.ofEpochSecond(epochtime, 0, ZoneOffset.UTC), ZoneId.of("UTC"));
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format.toStringUtf8());
        return utf8Slice(timestamp.format(formatter));
    }
}