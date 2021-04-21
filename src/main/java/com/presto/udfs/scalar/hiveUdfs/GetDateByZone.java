package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.udfs.utils.JsonUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class GetDateByZone {

    private GetDateByZone(){

    }

    private static final String PATTERN = "yyyyMMddHHmmss";
    private static final String ZONE_EXCHANGE = "{\"Europe/Ulyanovsk\":\"Europe/Moscow\", \"America/Punta_Arenas\":\"America/Santiago\", \"Asia/Yangon\":\"Asia/Rangoon\", \"Asia/Famagusta\":\"Asia/Nicosia\", \"Europe/Astrakhan\":\"Europe/Moscow\", \"Asia/Qostanay\":\"Asia/Qyzylorda\", \"Asia/Barnaul\":\"Asia/Novokuznetsk\", \"Asia/Atyrau\":\"Asia/Oral\", \"Europe/Saratov\":\"Europe/Volgograd\", \"Europe/Kirov\":\"Europe/Volgograd\", \"Asia/Tomsk\":\"Asia/Novosibirsk\"}";

    private static Map<String, String> zoneMap = null;
    static {
        zoneMap = JsonUtil.json2Obj(ZONE_EXCHANGE, Map.class);
    }

    @ScalarFunction("get_date_by_zone")
    @Description("get date by zone")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getDateByZone(@SqlType(StandardTypes.VARCHAR) Slice timestampSlice,
                                      @SqlType(StandardTypes.VARCHAR) Slice zoneNameSlice) {
        if (timestampSlice == null || zoneNameSlice == null) {
            return null;
        }

        String timestamp = timestampSlice.toStringUtf8();
        String zoneName = zoneNameSlice.toStringUtf8();

        // 由于 java8 收录的时区，故在此转换为已收录的时区
        if (zoneMap.containsKey(zoneName)) {
            zoneName = zoneMap.get(zoneName);
        }

        String to = null;
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(PATTERN);
            LocalDateTime localDateTime = LocalDateTime.parse(timestamp, formatter);
            ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
            ZonedDateTime toZoneDateTime = zonedDateTime.withZoneSameInstant(ZoneId.of(zoneName));
            to = toZoneDateTime.format(formatter);
        } catch (Exception e) {
            return null;
        }

        return Slices.utf8Slice(to);
    }
}
