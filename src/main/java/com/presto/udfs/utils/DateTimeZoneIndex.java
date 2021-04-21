package com.presto.udfs.utils;

import com.facebook.presto.common.type.DateTimeEncoding;
import com.facebook.presto.common.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import static com.facebook.presto.common.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.common.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.common.type.TimeZoneKey.MAX_TIME_ZONE_KEY;
import static com.facebook.presto.common.type.TimeZoneKey.getTimeZoneKeys;

public final class DateTimeZoneIndex {
    private DateTimeZoneIndex()
    {
    }



    private static final DateTimeZone[] DATE_TIME_ZONES;
    private static final ISOChronology[] CHRONOLOGIES;
    private static final int[] FIXED_ZONE_OFFSET;

    private static final int VARIABLE_ZONE = Integer.MAX_VALUE;

    static {
        DATE_TIME_ZONES = new DateTimeZone[MAX_TIME_ZONE_KEY + 1];
        CHRONOLOGIES = new ISOChronology[MAX_TIME_ZONE_KEY + 1];
        FIXED_ZONE_OFFSET = new int[MAX_TIME_ZONE_KEY + 1];
        for (TimeZoneKey timeZoneKey : getTimeZoneKeys()) {
            short zoneKey = timeZoneKey.getKey();
            DateTimeZone dateTimeZone = DateTimeZone.forID(timeZoneKey.getId());
            DATE_TIME_ZONES[zoneKey] = dateTimeZone;
            CHRONOLOGIES[zoneKey] = ISOChronology.getInstance(dateTimeZone);
            if (dateTimeZone.isFixed() && dateTimeZone.getOffset(0) % 60_000 == 0) {
                FIXED_ZONE_OFFSET[zoneKey] = dateTimeZone.getOffset(0) / 60_000;
            }
            else {
                FIXED_ZONE_OFFSET[zoneKey] = VARIABLE_ZONE;
            }
        }
    }

    public static ISOChronology getChronology(TimeZoneKey zoneKey)
    {
        return CHRONOLOGIES[zoneKey.getKey()];
    }

    public static ISOChronology unpackChronology(long timestampWithTimeZone)
    {
        return getChronology(unpackZoneKey(timestampWithTimeZone));
    }

    public static DateTimeZone getDateTimeZone(TimeZoneKey zoneKey)
    {
        return DATE_TIME_ZONES[zoneKey.getKey()];
    }

    public static DateTimeZone unpackDateTimeZone(long dateTimeWithTimeZone)
    {
        return getDateTimeZone(unpackZoneKey(dateTimeWithTimeZone));
    }

    public static long packDateTimeWithZone(DateTime dateTime)
    {
        return DateTimeEncoding.packDateTimeWithZone(dateTime.getMillis(), dateTime.getZone().getID());
    }

    public static int extractZoneOffsetMinutes(long dateTimeWithTimeZone)
    {
        short zoneKey = unpackZoneKey(dateTimeWithTimeZone).getKey();

        if (FIXED_ZONE_OFFSET[zoneKey] == VARIABLE_ZONE) {
            return DATE_TIME_ZONES[zoneKey].getOffset(unpackMillisUtc(dateTimeWithTimeZone)) / 60_000;
        }
        else {
            return FIXED_ZONE_OFFSET[zoneKey];
        }
    }
}
