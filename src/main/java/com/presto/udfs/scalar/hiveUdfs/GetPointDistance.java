package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.common.type.StandardTypes;
import com.presto.tools.gis.Point;

public class GetPointDistance {

    // 公
    private static final double R = 6372.8;

    private GetPointDistance(){

    }

    @ScalarFunction(value = "get_point_distance")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double evaluate(@SqlType(StandardTypes.BIGINT) long geohash_of_point1,@SqlType(StandardTypes.BIGINT) long geohash_of_point2) {
        Point point1 = Point.fromGeoHash(geohash_of_point1);
        Point point2 = Point.fromGeoHash(geohash_of_point2);
        return evaluate(point1.x, point1.y, point2.x, point2.y);
    }


    @ScalarFunction(value = "get_point_distance")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double evaluate(@SqlType(StandardTypes.BIGINT) long geohash_of_point1,
                           @SqlType(StandardTypes.DOUBLE) double latitude_of_point2,
                           @SqlType(StandardTypes.DOUBLE) double longitude_of_point2) {
        Point point1 = null;
        try{
            point1 = Point.fromGeoHash(geohash_of_point1);
        }catch(Exception ex){
            System.out.println("Error while process geohash "+geohash_of_point1 );
            return null;
        }

        return evaluate(point1.x, point1.y, latitude_of_point2, longitude_of_point2);
    }


    @ScalarFunction(value = "get_point_distance")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double evaluate(@SqlType(StandardTypes.DOUBLE) double latitude_of_point1,
                                  @SqlType(StandardTypes.DOUBLE) double longitude_of_point1,
                                  @SqlType(StandardTypes.DOUBLE) double latitude_of_point2,
                                  @SqlType(StandardTypes.DOUBLE) double longitude_of_point2) {
        if ( Double.valueOf(latitude_of_point1) == null
                || Double.valueOf(longitude_of_point1) == null
                || Double.valueOf(latitude_of_point2) == null
                || Double.valueOf(longitude_of_point2) == null
                || Math.abs(latitude_of_point1) > 90
                || Math.abs(longitude_of_point1) > 180
                || Math.abs(latitude_of_point2) > 90
                || Math.abs(longitude_of_point2) > 180) {
            return null;
        }

        double dLat = Math.toRadians(latitude_of_point2 - latitude_of_point1);
        double dLon = Math.toRadians(longitude_of_point2 - longitude_of_point1);
        double lat1 = Math.toRadians(latitude_of_point1);
        double lat2 = Math.toRadians(latitude_of_point2);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));

        return new Double(R * c);
    }
}
