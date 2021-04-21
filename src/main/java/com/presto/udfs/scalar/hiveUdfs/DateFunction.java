package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import  com.presto.udfs.utils.DateTimeZoneIndex;
import com.google.common.base.Charsets;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;


public final class DateFunction {
    private DateFunction() {}

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.UTC);
    private static final org.joda.time.format.DateTimeFormatter format1 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final org.joda.time.format.DateTimeFormatter format2 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
    private static final org.joda.time.format.DateTimeFormatter format3 = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final org.joda.time.format.DateTimeFormatter format4 = DateTimeFormat.forPattern("yyyyMMdd");
    private static final org.joda.time.format.DateTimeFormatter format5 = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");

    @Description("Returns the date part of the date string")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringTimestampToDate(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp)
    {

        //TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(inputTimestamp.toStringUtf8()).toString("yyyy-MM-dd");
        //String result=parseTimeString(inputTimestamp).toString("yyyy-MM-dd");
        //return Slices.copiedBuffer(result, Charsets.UTF_8);
        String input = inputTimestamp.toStringUtf8();
        if(input.length()>19){
            input = input.substring(0,19);
        }
        return Slices.utf8Slice(TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(input).toString("yyyy-MM-dd"));
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


    @Description("Returns the date part of the timestamp")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timestampToDate(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp)
    {

        //return formatDatetime(session, timestamp, utf8Slice("yyyy-MM-dd"));
        return formatDatetime2(DateTimeZoneIndex.getChronology(properties.getTimeZoneKey()), properties.getSessionLocale(), timestamp, utf8Slice("yyyy-MM-dd"));
    }

    private static Slice formatDatetime2(ISOChronology chronology, Locale locale, long timestamp, Slice formatString) {
        try {
            return Slices.utf8Slice(DateTimeFormat.forPattern(formatString.toStringUtf8()).withChronology(chronology).withLocale(locale).print(timestamp));
        } catch (IllegalArgumentException var6) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, var6);
        }
    }


    public static long timeZoneHourFromTimestampWithTimeZone( long timestampWithTimeZone) {
        return (long)(DateTimeZoneIndex.extractZoneOffsetMinutes(timestampWithTimeZone) / 60);
    }
    public static long timeZoneMinuteFromTimestampWithTimeZone(long timestampWithTimeZone) {
        return (long)(DateTimeZoneIndex.extractZoneOffsetMinutes(timestampWithTimeZone) % 60);
    }
    public static double toUnixTimeFromTimestampWithTimeZone( long timestampWithTimeZone) {
        return (double) DateTimeEncoding.unpackMillisUtc(timestampWithTimeZone) / 1000.0D;
    }


    @Description("Returns the date part of the timestamp with time zone")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timestampWithTimeZoneToDate(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestamp)
    {
        // Offset is added to the unix timestamp to incorporate the affect of Timezone.
        long offset = ((timeZoneHourFromTimestampWithTimeZone(timestamp) * 60 + timeZoneMinuteFromTimestampWithTimeZone(timestamp)) * 60) * 1000;
        //return formatDatetime2(session, ((long) toUnixTimeFromTimestampWithTimeZone(timestamp) * 1000) + offset, utf8Slice("yyyy-MM-dd"));
        return formatDatetime2(DateTimeZoneIndex.getChronology(properties.getTimeZoneKey()), properties.getSessionLocale(),
                ((long) toUnixTimeFromTimestampWithTimeZone(timestamp) * 1000) + offset, utf8Slice("yyyy-MM-dd"));
    }

    @Description("Returns the date part of the date string with format")
    @ScalarFunction("to_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice stringTimestampToDate(@SqlType(StandardTypes.VARCHAR) Slice inputTimestamp,@SqlType(StandardTypes.VARCHAR) Slice format)
    {
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S ");

        LocalDate date = LocalDate.parse(inputTimestamp.toStringUtf8(), formatter);
        return utf8Slice(date.format(java.time.format.DateTimeFormatter.ofPattern(format.toString())));
    }

    @ScalarFunction("get_date")
    @Description("return the String date return negative values.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date()
    {
        LocalDate now = LocalDate.now();
        String ret = now.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        return Slices.utf8Slice(ret);
    }

    @ScalarFunction("get_date")
    @Description("return the String date return negative values.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date(@SqlType(StandardTypes.VARCHAR) Slice time, @SqlType(StandardTypes.BIGINT) long offset)
    {
        return get_date(time,offset,Slices.utf8Slice("D"));
    }

    @ScalarFunction("get_date")
    @Description("return the String date for push druid. format : 2019-07-01T00:00:00.000Z")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date(@SqlType(StandardTypes.VARCHAR) Slice timeSlice, @SqlType(StandardTypes.VARCHAR) Slice formatSlice){
        String format = formatSlice.toStringUtf8();
        String timeExp = timeSlice.toStringUtf8();
        try {
            DateTime dateTime = DateTimeFormat.forPattern(format).parseDateTime(timeExp);
            String print = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").print(dateTime);
            String ret = print.replace(" ", "T") + ".000Z";
            return Slices.utf8Slice(ret);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 比照当前时间获取日期， 如getDate(-1) 为昨天，getDate(1) 为明天
     *
     * @param days 比当前时间的偏移天数
     * @return 日期字符
     */
    @ScalarFunction("get_date")
    @Description("return the String date return negative values.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date(@SqlType(StandardTypes.BIGINT)long days)
    {
        LocalDate now = LocalDate.now();
        LocalDate localDate = now.plusDays(days);
        String ret = localDate.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        return Slices.utf8Slice(ret);
    }


    /**
     * 将日期20130606的格式转换成2013-06-06
     *
     * @return 日期字符
     */
    @ScalarFunction("get_date")
    @Description("return the rn negative values.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date(@SqlType(StandardTypes.VARCHAR) Slice time)
    {
        String timeExp = time.toString(Charsets.UTF_8);
        String formatString = getFormatString(timeExp);
        if (formatString == null) {
            return null;
        }
        return Slices.copiedBuffer(formatString, Charsets.UTF_8);
    }

    private static String getFormatString(String timeExp){
        if(timeExp==null) {
            return null;
        }
        String[] split = timeExp.split("\\s");
        if(timeExp.indexOf("-") == -1) {
            if (split[0].length() < 8) {
                return null;
            }
            String dateStr = timeExp.substring(0,4);
            dateStr = dateStr + '-' + timeExp.substring(4,6);
            dateStr = dateStr + '-' + timeExp.substring(6,8);
            return dateStr;
        }else{
            if (split[0].length() < 10) {
                return null;
            }
            return timeExp.substring(0, 10);
        }
    }

    @ScalarFunction("get_date")
    @Description("return the Offset time values.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date(@SqlType(StandardTypes.BIGINT) long offset, @SqlType(StandardTypes.VARCHAR) Slice units)
    {
        String unit = units.toString(Charsets.UTF_8);
        LocalDate now = LocalDate.now();
        String ret = getOffsetTime(now, offset, unit);
        return Slices.copiedBuffer(ret, Charsets.UTF_8);
    }

    private static String getOffsetTime(LocalDate curDate, long offset, String unit){
        LocalDate localDate = null;
        java.time.format.DateTimeFormatter formatter = null;
        if(unit.startsWith("y")|| unit.startsWith("Y")) {
            localDate = curDate.plusYears(offset);
            formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy");
        }else if(unit.startsWith("M")||unit.startsWith("m")){
            localDate = curDate.plusMonths(offset);
            formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMM");
        }else{
            localDate = curDate.plusDays(offset);
            formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd");
        }
        return localDate.format(formatter);
    }

    @ScalarFunction("get_date")
    @Description("return the Offset time values.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice get_date(@SqlType(StandardTypes.VARCHAR) Slice time, @SqlType(StandardTypes.BIGINT) long offset,
                           @SqlType(StandardTypes.VARCHAR) Slice units) {
        if (time == null) {
            return null;
        }

        String timeExp= time.toString(Charsets.UTF_8);
        String unit = units.toString(Charsets.UTF_8);
        if (timeExp == null) {
            return null;
        }
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");
        timeExp = getFormatString(timeExp);
        if (timeExp == null) {
            return null;
        }
        LocalDate parse = LocalDate.parse(timeExp, formatter);

        String ret = getOffsetTime(parse, offset, unit);
        return Slices.copiedBuffer(ret, Charsets.UTF_8);
    }

    @ScalarFunction(value = "get_date")
    @Description("返回根据开始日期和结束日期得到对应格式的日期列表："
            + "\n   * suffix(假设suffix为*)不为空 分为四种情况：\n" +
            "       * （1）日期差大于一年，返回格式为yyyy*的列表\n" +
            "       * （2）日期差大于一个月，返回格式为yyyyMM*的列表\n" +
            "       * （3）日期差大于10天，返回格式为yyyyMMd*的列表\n" +
            "       * （4）日期差为1，返回开始日期\n" +
            "       * suffix为空 分为一种情况：\n" +
            "       * （1）返回格式为yyyyMM的月份列表")
    @SqlNullable
    @SqlType("array(varchar)")
    public static Block getDateList(@SqlType(StandardTypes.VARCHAR) Slice startDate,
                                    @SqlType(StandardTypes.VARCHAR) Slice endDate,
                                    @SqlType(StandardTypes.VARCHAR) Slice suffix){
        String suffixStr = null;
        if (null != suffix) {
            suffixStr = suffix.toStringUtf8();
        }
        List<String> list = getDateList(startDate.toStringUtf8(), endDate.toStringUtf8(), suffixStr);
        int len = list.size();
        if (len == 0) {
            return null;
        }
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, len);
        for (String str: list) {
            VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(str));
        }
        return blockBuilder;
    }

    private static List<String> getDateList(String startDate, String endDate, String suffix){
        List<String> list = new ArrayList<>();
        if (StringUtils.isBlank(startDate) || StringUtils.isBlank(endDate)){
            return list;
        }
        LocalDate start = null;
        LocalDate end = null;
        try {
            start = LocalDate.parse(startDate, java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
            end = LocalDate.parse(endDate, java.time.format.DateTimeFormatter.BASIC_ISO_DATE);
        } catch (Exception e) {
            return list;
        }

        int index = -1;
        ChronoUnit unit = null;
        int offset = -1;
        String format = null;
        if (StringUtils.isNotBlank(suffix)){
            format = "%s"+suffix;
            long diff = start.until(end, ChronoUnit.DAYS);
            if (diff == 1){
                list.add(startDate);
                return list;
            } else if(diff >= 365){
                offset = 1;
                index = 4;
                unit = ChronoUnit.YEARS;
            } else if(diff >= 28){
                offset = 1;
                index = 6;
                unit = ChronoUnit.MONTHS;
            } else if(diff >= 10){
                offset = 10;
                index = 7;
                unit = ChronoUnit.DAYS;
            }
        } else {
            format = "%s";
            offset = 1;
            index = 6;
            unit = ChronoUnit.MONTHS;
        }
        while(start.isBefore(end)){
            startDate = start.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
            list.add(String.format(format, startDate.substring(0, index)));
            start = start.plus(offset, unit);
        }
        return list;
    }

    /**
     * 计算两个时间字符串之间的时间差，单位为秒数
     *
     */
    @ScalarFunction("time_diff")
    @Description("return the seconds between two date-time syntax")
    @SqlType(StandardTypes.BIGINT)
    public static long time_diff(@SqlType(StandardTypes.VARCHAR) Slice date1, @SqlType(StandardTypes.VARCHAR) Slice date2)
    {
        String t1 = date1.toStringUtf8();
        if(t1.length()>19){
            t1 = t1.substring(0,19);
        }
        String t2 = date2.toStringUtf8();
        if(t2.length()>19){
            t2 = t2.substring(0,19);
        }

        Long secondstime1 = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(t1).getMillis() / 1000;
        Long secondstime2 = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(t2).getMillis() / 1000;
        return secondstime2 - secondstime1;
    }


    @Description("parses the specified date/time by the given format")
    @ScalarFunction("parse_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parse_date(SqlFunctionProperties properties, @SqlType(StandardTypes.VARCHAR) Slice datetime, @SqlType(StandardTypes.VARCHAR) Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        org.joda.time.format.DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern)
                .withChronology(DateTimeZoneIndex.getChronology(properties.getTimeZoneKey()))
                .withOffsetParsed()
                .withLocale(properties.getSessionLocale());

        String datetimeString = datetime.toString(Charsets.UTF_8);
        return Slices.copiedBuffer(parseDateTimeHelper(formatter, datetimeString).toString(), Charsets.UTF_8);
    }
    @Description("parses the specified date/time by the given format")
    @ScalarFunction("parse_date")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice parse_date(SqlFunctionProperties properties, @SqlType(StandardTypes.VARCHAR) Slice datetime)
    {
        String datetimeString = datetime.toString(Charsets.UTF_8);
        return Slices.copiedBuffer(parseDateTimeHelper(format1, datetimeString).toString(), Charsets.UTF_8);
    }

    private static DateTime parseDateTimeHelper(DateTimeFormatter formatter, String datetimeString)
    {
        try {
            return formatter.parseDateTime(datetimeString);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static DateTime parseTimeString(Slice timeExp) {
        String time = timeExp.toString(Charsets.UTF_8);;
        DateTime resDate=null;
        boolean bParsed= false;
        try{
            resDate = format1.parseDateTime(time);
            bParsed= true;
        }catch(Exception ex1){}

        if(!bParsed){
            try{
                resDate = format2.parseDateTime(time);
                bParsed= true;
            }catch(Exception ex1){}
        }
        if(!bParsed){
            try{
                resDate = format3.parseDateTime(time);
                bParsed= true;
            }catch(Exception ex1){}
        }
        if(!bParsed){
            try{
                resDate = format4.parseDateTime(time);
                bParsed= true;
            }catch(Exception ex1){}
        }
        if(!bParsed){
            try{
                resDate = format5.parseDateTime(time);
                bParsed= true;
            }catch(Exception ex1){}
        }

        if(!bParsed){
//            log.warn("Failed to Recognize the date expression "+ time+".");
//            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
            return null;
        }
        return resDate;
    }

//    @ScalarFunction("unix_timestamp")
//    @Description("return unix timestamp")
//    @SqlType(StandardTypes.VARCHAR)
//    public static Slice unix_timestamp(@SqlType(StandardTypes.VARCHAR) Slice time) {
//        if (time == null) {
//            return null;
//        }
//
//        String timeExp = time.toString(Charsets.UTF_8);
//        Object timestampOfDateTime = 0;
//        try {
//            timestampOfDateTime = getTimestampOfDateTime(timeExp, "yyyy-MM-dd HH:mm:ss");
//            return Slices.copiedBuffer(String.valueOf(timestampOfDateTime), Charsets.UTF_8);
//        } catch (Exception e) {
//            return null;
//        }
//    }

    @ScalarFunction("unix_timestamp")
    @Description("return unix timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long unix_timestamp(@SqlType(StandardTypes.VARCHAR) Slice time) {
        if (time == null) {
            return 0L;
        }

        String timeExp = time.toString(Charsets.UTF_8);
        try {
            Object timestampOfDateTime = getTimestampOfDateTime(timeExp, "yyyy-MM-dd HH:mm:ss");
            if (timestampOfDateTime == null) {
                return 0L;
            }
            return ((LongWritable)timestampOfDateTime).get();
        } catch (Exception e) {
            return 0L;
        }
    }

//    @ScalarFunction("unix_timestamp")
//    @Description("return unix timestamp")
//    @SqlType(StandardTypes.VARCHAR)
//    public static Slice unix_timestamp(@SqlType(StandardTypes.VARCHAR) Slice time,
//                                       @SqlType(StandardTypes.VARCHAR) Slice format) {
//        if (time == null || format == null) {
//            return null;
//        }
//
//        String timeExp = time.toString(Charsets.UTF_8);
//        String formatExp = format.toString(Charsets.UTF_8);
//        try {
//            Object timestampOfDateTime = getTimestampOfDateTime(timeExp, formatExp);
//            return Slices.copiedBuffer(String.valueOf(timestampOfDateTime), Charsets.UTF_8);
//        } catch (Exception e) {
//            return null;
//        }
//    }

    @ScalarFunction("unix_timestamp")
    @Description("return unix timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long unix_timestamp(@SqlType(StandardTypes.VARCHAR) Slice time,
                                      @SqlType(StandardTypes.VARCHAR) Slice format) {
        if (time == null || format == null) {
            return 0L;
        }

        String timeExp = time.toString(Charsets.UTF_8);
        String formatExp = format.toString(Charsets.UTF_8);
        try {
            Object timestampOfDateTime = getTimestampOfDateTime(timeExp, formatExp);
            if (timestampOfDateTime == null) {
                return 0L;
            }
            return ((LongWritable)timestampOfDateTime).get();
        } catch (Exception e) {
            return 0L;
        }
    }

//    @ScalarFunction("unix_timestamp")
//    @Description("return unix timestamp")
//    @SqlType(StandardTypes.VARCHAR)
//    public static Slice unix_timestamp(@SqlType(StandardTypes.DATE) Date time,
//                                       @SqlType(StandardTypes.VARCHAR) Slice format) {
//        if (time == null || format == null) {
//            return null;
//        }
//        try {
//            return Slices.copiedBuffer(String.valueOf(time.getTime() / 1000), Charsets.UTF_8);
//        } catch (Exception e) {
//            return null;
//        }
//    }


    @ScalarFunction("unix_timestamp")
    @Description("return unix timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long unix_timestamp(@SqlType(StandardTypes.DATE) Date time,
                                       @SqlType(StandardTypes.VARCHAR) Slice format) {
        if (time == null || format == null) {
            return 0L;
        }
        try {
            return time.getTime() / 1000;
        } catch (Exception e) {
            return 0L;
        }
    }

//    @ScalarFunction("unix_timestamp")
//    @Description("return unix timestamp")
//    @SqlType(StandardTypes.VARCHAR)
//    public static Slice unix_timestamp(@SqlType(StandardTypes.TIMESTAMP) Timestamp time,
//                                       @SqlType(StandardTypes.VARCHAR) Slice format) {
//        if (time == null || format == null) {
//            return null;
//        }
//        try {
//            return Slices.copiedBuffer(String.valueOf(time.getTime() / 1000), Charsets.UTF_8);
//        } catch (Exception e) {
//            return null;
//        }
//    }

    @ScalarFunction("unix_timestamp")
    @Description("return unix timestamp")
    @SqlType(StandardTypes.BIGINT)
    public static long unix_timestamp(@SqlType(StandardTypes.TIMESTAMP) Timestamp time,
                                       @SqlType(StandardTypes.VARCHAR) Slice format) {
        if (time == null || format == null) {
            return 0L;
        }
        try {
            return time.getTime() / 1000;
        } catch (Exception e) {
            return 0L;
        }
    }

    private static Object getTimestampOfDateTime(String timeExp, String format){
//        if (timeExp.contains(".")) {
//            timeExp = timeExp.substring(0, timeExp.indexOf("."));
//        }
//
//        if (!format.contains("MM")) {
//            format = format + "MM-dd HH:mm:ss";
//            timeExp = timeExp + "01-01 00:00:00";
//        } else if (!format.contains("dd")) {
//            format = format + "-dd HH:mm:ss";
//            timeExp = timeExp + "-01 00:00:00";
//        } else if (!format.contains("HH")){
//            format = format + " HH:mm:ss";
//            timeExp = timeExp + " 00:00:00";
//        }
//
//        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern(format);
//        LocalDateTime parse = LocalDateTime.parse(timeExp, formatter);
//        long timestampOfDateTime = getTimestampOfDateTime(parse);
//        return timestampOfDateTime;
        LongWritable retValue = new LongWritable();
        String lasPattern = "yyyy-MM-dd HH:mm:ss";
        final SimpleDateFormat formatter = new SimpleDateFormat(lasPattern);
        formatter.setTimeZone(TimeZone.getDefault());
        if (!format.equals(lasPattern)) {
            formatter.applyPattern(format);
        }

        try {
            retValue.set(formatter.parse(timeExp).getTime() / 1000);
            return retValue;


        } catch (ParseException e) {
            return null;
        }
    }

    private static long getTimestampOfDateTime(LocalDateTime localDateTime) {
        ZoneId zone = ZoneId.systemDefault();
        Instant instant = localDateTime.atZone(zone).toInstant();
        return instant.toEpochMilli();
    }

}