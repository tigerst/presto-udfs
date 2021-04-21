package com.presto.tools.gis;

import com.presto.tools.gis.geohash.WGS84Point;

public class Point {

    public double x;
    public double y;
    public Point(double x, double y){
        this.x=x;
        this.y=y;
    }


    public static Point fromGeoHash(Long geohash){
        Long temp= (long)(Math.pow(2, 63))+geohash+1;
        String val= Long.toHexString(temp);

        StringBuilder sb=new StringBuilder();
        for(int i=0; i<val.length();i+=2){
            int a= Integer.parseInt(val.substring(i, i+2), 16);
            sb.append((char)a);
        }
        WGS84Point point= com.presto.tools.gis.geohash.GeoHash.fromGeohashString(sb.toString().toLowerCase()).getPoint();
        return new Point(point.getLatitude(), point.getLongitude());
    }

    public static Point fromGeoHash(String geohash){
        WGS84Point point= com.presto.tools.gis.geohash.GeoHash.fromGeohashString(geohash.toLowerCase()).getPoint();
        return new Point(point.getLatitude(), point.getLongitude());
    }

    @Override
    public String toString(){
        return "LAT: " + this.x + ",  LNG: " + this.y;
    }




}
