package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.tools.gis.GeoHash;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

@ScalarFunction(value = "geohash_of_latlng")
public class GeohashOfLatLng {
    private GeohashOfLatLng(){}


    @SqlType(StandardTypes.VARCHAR)
    public static Slice geohash(@SqlType(StandardTypes.DOUBLE)  double lat,
                                @SqlType(StandardTypes.DOUBLE)  double lng)
    {
        return Slices.utf8Slice(GeoHash.encode(lat, lng));
    }


}
