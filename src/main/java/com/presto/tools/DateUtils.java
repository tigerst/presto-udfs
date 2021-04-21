package com.presto.tools;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;
import java.util.Date;

public class DateUtils {


    public static enum DateUnit {
        YEAR,
        MONTH,
        DAY
    }
    public static ThreadLocal<SimpleDateFormat> dateFormat1 = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
    };
    public static ThreadLocal<SimpleDateFormat> dateFormat2 = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };
    public static ThreadLocal<SimpleDateFormat> dateFormat3 = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };
    public static ThreadLocal<SimpleDateFormat> dateFormat4 = new ThreadLocal<SimpleDateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyyMMdd");
        }
    };

    public static Date parseTimeString(String time) {
        time=time.trim();
        Date resDate = null;
        boolean bParsed = false;
        try {
            resDate = dateFormat1.get().parse(time);
            bParsed = true;
        } catch (Exception ex1) {}

        if (!bParsed) {
            try {
                resDate = dateFormat2.get().parse(time);
                bParsed = true;
            } catch (Exception ex1) {}
        }

        if (!bParsed) {
            try {
                resDate = dateFormat3.get().parse(time);
                bParsed = true;
            } catch (Exception ex1) {}
        }

        if (!bParsed) {
            return null;
        }
        return resDate;
    }


    public static Long parseTimeToSeconds(String time) {
        if (time == null || time.trim().length() == 0) {
            return null;
        }
        Date date = parseTimeString(time.trim());
        if (date==null) {
            return null;
        }
        return date.getTime() / 1000;
    }


    public static String getOffsetDate(Date date, Integer offset, DateUnit unit) {
        String today = DateUtils.dateFormat3.get().format(date);
        String[] datePart= today.split("-");
        int year = Integer.parseInt(datePart[0]);
        int month = Integer.parseInt(datePart[1]);
        int day = Integer.parseInt(datePart[2]);
        int[] dateArray = new int[]{year, month, day};

        if (unit==DateUnit.YEAR) {
            return new Integer(dateArray[0]+offset).toString();
        }
        if (unit==DateUnit.MONTH) {
            dateArray[1] = dateArray[1]+ offset;
            if (dateArray[1] <= 0 || dateArray[1] > 12) {
                int numY=dateArray[1] / 12;
                int numM = dateArray[1] % 12;
                dateArray[0] = dateArray[0] + numY;
                dateArray[1] = numM;
                if (dateArray[1] <= 0) {
                    dateArray[0] = dateArray[0] - 1;
                    dateArray[1] = dateArray[1] + 12;
                }
            }
            if (dateArray[1] < 10) {
                return dateArray[0]+ "-0" + dateArray[1];
            } else {
                return dateArray[0]+ "-" + dateArray[1];
            }
        }
        long res = date.getTime() + 1000 * 3600 * 24 * offset.longValue();
        return DateUtils.dateFormat3.get().format(new Date(res));
    }

    /**
     * 计算两个日期相差的天数
     * 日期格式为yyyyMMdd
     * @param startDate
     * @param endDate
     * @return
     */
    public static long getBetweenVal(String startDate, String endDate, TemporalUnit unit){
        if (startDate.length() != 8 || endDate.length() != 8){
            return -1;
        }
        LocalDate start = LocalDate.parse(startDate, DateTimeFormatter.BASIC_ISO_DATE);
        LocalDate end = LocalDate.parse(endDate, DateTimeFormatter.BASIC_ISO_DATE);
        return unit.between(start, end);
    }

    /**
     * 计算日期偏移后的值
     * @param date
     * @param offset
     * @param unit
     * @return
     */
    public static LocalDate getOffsetVal(LocalDate date, long offset, TemporalUnit unit){
        return date.plus(offset, unit);
    }

    /**
     * 获取当前时间
     * @return
     */
    public static String curDate(){
        LocalDate now = LocalDate.now();
        String format = now.format(DateTimeFormatter.BASIC_ISO_DATE);
        return format;
    }
}
