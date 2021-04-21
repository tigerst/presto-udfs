package com.presto.udfs.utils;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.presto.udfs.utils.DateTimeZoneIndex.*;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

public final class DateTimeFunctions {
	private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(UTC);

	private DateTimeFunctions() {
	}

	public static long timeZoneHourFromTimestampWithTimeZone(long timestampWithTimeZone) {
		return extractZoneOffsetMinutes(timestampWithTimeZone) / 60;
	}

	public static long addFieldValueDate(Slice unit, long value, long date) {
		long millis = getDateField(UTC_CHRONOLOGY, unit).add(DAYS.toMillis(date), toIntExact(value));
		return MILLISECONDS.toDays(millis);
	}

	private static DateTimeField getDateField(ISOChronology chronology, Slice unit) {
		String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
		switch (unitString) {
			case "day":
				return chronology.dayOfMonth();
			case "week":
				return chronology.weekOfWeekyear();
			case "month":
				return chronology.monthOfYear();
			case "quarter":
				return QuarterOfYearDateTimeField.QUARTER_OF_YEAR.getField(chronology);
			case "year":
				return chronology.year();
		}
		throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid DATE field");
	}

	public static Slice toISO8601FromDate(long date) {
		DateTimeFormatter formatter = ISODateTimeFormat.date()
				.withChronology(UTC_CHRONOLOGY);
		return utf8Slice(formatter.print(DAYS.toMillis(date)));
	}

	public static long timeZoneMinuteFromTimestampWithTimeZone(long timestampWithTimeZone) {
		return extractZoneOffsetMinutes(timestampWithTimeZone) % 60;
	}

	public static long weekFromTimestamp(SqlFunctionProperties properties, long timestamp) {
		return getChronology(properties.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
	}

	public static long weekFromTimestampWithTimeZone(long timestampWithTimeZone) {
		return unpackChronology(timestampWithTimeZone).weekOfWeekyear().get(unpackMillisUtc(timestampWithTimeZone));
	}

	public static long diffDate(Slice unit, long date1, long date2) {
		return getDateField(UTC_CHRONOLOGY, unit).getDifferenceAsLong(DAYS.toMillis(date2), DAYS.toMillis(date1));
	}

	public static long diffTimestamp(SqlFunctionProperties properties, Slice unit, long timestamp1, long timestamp2) {
		return getTimestampField(getChronology(properties.getTimeZoneKey()), unit).getDifferenceAsLong(timestamp2, timestamp1);
	}

	private static DateTimeField getTimestampField(ISOChronology chronology, Slice unit) {
		String unitString = unit.toStringUtf8().toLowerCase(ENGLISH);
		switch (unitString) {
			case "millisecond":
				return chronology.millisOfSecond();
			case "second":
				return chronology.secondOfMinute();
			case "minute":
				return chronology.minuteOfHour();
			case "hour":
				return chronology.hourOfDay();
			case "day":
				return chronology.dayOfMonth();
			case "week":
				return chronology.weekOfWeekyear();
			case "month":
				return chronology.monthOfYear();
			case "quarter":
				return QuarterOfYearDateTimeField.QUARTER_OF_YEAR.getField(chronology);
			case "year":
				return chronology.year();
		}
		throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "'" + unitString + "' is not a valid Timestamp field");
	}

	public static long diffTimestampWithTimeZone(Slice unit, long timestampWithTimeZone1, long timestampWithTimeZone2) {
		return getTimestampField(unpackChronology(timestampWithTimeZone1), unit).getDifferenceAsLong(unpackMillisUtc(timestampWithTimeZone2), unpackMillisUtc(timestampWithTimeZone1));
	}

	public static Slice formatDatetime(SqlFunctionProperties properties, @SqlType(StandardTypes.TIMESTAMP) long timestamp, @SqlType("varchar(x)") Slice formatString)
	{
		return formatDatetime(getChronology(properties.getTimeZoneKey()), properties.getSessionLocale(), timestamp, formatString);
	}

	private static Slice formatDatetime(ISOChronology chronology, Locale locale, long timestamp, Slice formatString)
	{
		try {
			return utf8Slice(DateTimeFormat.forPattern(formatString.toStringUtf8())
					.withChronology(chronology)
					.withLocale(locale)
					.print(timestamp));
		}
		catch (IllegalArgumentException e) {
			throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
		}
	}

	public static double toUnixTimeFromTimestampWithTimeZone(@SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE) long timestampWithTimeZone)
	{
		return unpackMillisUtc(timestampWithTimeZone) / 1000.0;
	}
}

