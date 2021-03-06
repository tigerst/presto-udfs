package com.presto.udfs.scalar.date;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.utils.ConfigUtils;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.util.Calendar;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * 1: 法定节假日, 2: 正常周末, 3: 正常工作日 4:攒假的工作日
 *
 */
public class ChinaTypeOfDayFunction {

    public final static DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd");

    private ChinaTypeOfDayFunction(){

    }

    private enum DayType {
        /**
         * 法定节假日
         */
        HOLIDAY("holiday"),

        /**
         * 攒假的工作日
         */
        WORKDAY("workday");

        private String code;
        private DayType(String code) {
            this.code = code;
        }

        public String getCode() {
            return this.code;
        }
    }

    @ScalarFunction("typeOfDay")
    @Description("Returns the type of day from a date string(yyyy-MM-dd)")
    @SqlType(StandardTypes.BIGINT)
    public static long typeOfDay(@SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return -1;
        }

        String dateStr = string.toStringUtf8();
        try {
            String value = ConfigUtils.getInstance().getDateType(dateStr);
            if (DayType.HOLIDAY.getCode().equalsIgnoreCase(value)) {
                //1: 法定节假日
                return 1;
            } else if (DayType.WORKDAY.getCode().equalsIgnoreCase(value)) {
                //4:攒假的工作日
                return 4;
            } else {
                LocalDate date = LocalDate.parse(string.toStringUtf8(), DEFAULT_DATE_FORMATTER);
                if (date.getDayOfWeek() < 6) {
                    //正常工作日
                    return 3;
                } else {
                    //正常周末
                    return 2;
                }
            }
        } catch (Exception e) {
        }

        return -1;
    }

    @ScalarFunction("typeOfDay")
    @Description("Returns the day of week from a date string")
    @SqlType(StandardTypes.BIGINT)
    public static long typeOfDay(@SqlType(StandardTypes.DATE) long date) {
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(DAYS.toMillis(date));
            LocalDate localDate = LocalDate.fromCalendarFields(calendar);

            String dateStr = localDate.toString(DEFAULT_DATE_FORMATTER);
            return typeOfDay(Slices.utf8Slice(dateStr));
        } catch (Exception e) {
        }

        return -1;
    }
}
