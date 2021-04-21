package com.presto.udfs.geoip;

public interface FileResolve {

    long getQueryIpFileLen();

    long getRangeValueFromQueryIpFile(long start, long end);

    long getRangeValueFromDatafile(int start, int end);

    byte[] getRangeBytesFromQueryIpFile(int start, int end);

    byte[] getRangeBytesFromDatafile(long start, long end);

    GeoInfo decode(byte[] bytes);

}
