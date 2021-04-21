package com.presto.tools.gis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
public class CoveredGeohash {
    public static final int DEFAULT_ACCURACY = 7;
    private static char[] _base32 = { '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
    private final static Map<Character, Integer> _decodemap = new HashMap<Character, Integer>();
    private static final String LAT = "lat";
    private static final String LNG = "lng";
    static {
        int sz = _base32.length;
        for (int i = 0; i < sz; i++) {
            _decodemap.put(_base32[i], i);
        }
    }
    /**
     * Get all geoHash in vertex area with defined accuracy
     *
     * @param vertex
     *            : list [latitude1, longitude1, latitude2, longitude2, ...]
     * @param accuracy
     *            : length of geoHash
     * @return List [geohash1,geohash2,...]
     */
    public static List<String> getGeoHashList(List<Double> vertex,
                                              int accuracy) {
        return getGeoHashList(vertex, getRect(vertex), accuracy);
    }
    /**
     * Get all geoHash in vertex area with defined accuracy
     *
     * @param vertex
     *            : list [latitude1, longitude1, latitude2, longitude2, ...]
     * @param rect
     *            : size is 4, from vertex list [minLatitude,minLongitude,
     *            maxLatitude,maxLongitude]
     * @param accuracy
     *            : length of geoHash
     * @return List [geohash1,geohash2,...]
     */
    public static List<String> getGeoHashList(List<Double> vertex,
                                              List<Double> rect, int accuracy) {
        List<String> geoHashList = null;
        if (rect != null && vertex != null && rect.size() == 4
                && vertex.size() % 2 == 0) {
            // get all geoHash in max min location area
            geoHashList = getAllGeoHashList(rect, accuracy);
            Point p = null;
            String geoHash;
            double[] locations;
            // remove the geoHash not in vertex list
            Iterator<String> geoHashIter = geoHashList.iterator();
            while (geoHashIter.hasNext()) {
                geoHash = geoHashIter.next();
                locations = GeoHash.decode_exactly(geoHash);
                p = new Point(locations[0], locations[1]);
                if (GISCross.isInner(p, vertex, rect) != 1) {
                    geoHashIter.remove();
                }
            }
        }
        return geoHashList;
    }
    /**
     * Get all geoHash in the square between (minLatitude, minLongitude) and
     * (maxLatitude,maxLongitude)
     *
     * @param rect
     *            : size is 4, list [minLatitude,minLongitude,
     *            maxLatitude,maxLongitude]
     * @return LinkedList : remove action spent less time
     */
    private static List<String> getAllGeoHashList(List<Double> rect,
                                                  int accuracy) {
        List<String> geoHashList = new LinkedList<String>();
        double minLatitude = rect.get(0);
        double minLongitude = rect.get(1);
        double maxLatitude = rect.get(2);
        double maxLongitude = rect.get(3);
        String minGeoHash = GeoHash.encode(minLatitude, minLongitude);
        String maxGeoHash = GeoHash.encode(maxLatitude, maxLongitude);
        String minBinary[] = getBinaryLocation(
                toBinaryString(minGeoHash, accuracy));
        String maxBinary[] = getBinaryLocation(
                toBinaryString(maxGeoHash, accuracy));
        String geoHash;
        for (String i = minBinary[0]; i
                .compareTo(maxBinary[0]) <= 0; i = binaryAdd(i, "1")) {
            for (String j = minBinary[1]; j
                    .compareTo(maxBinary[1]) <= 0; j = binaryAdd(j, "1")) {
                geoHash = getGeoHash(i, j, accuracy);
                geoHashList.add(geoHash);
            }
        }
        return geoHashList;
    }
    private static String getGeoHash(String latitudeBinary,
                                     String longitudeBinary, int accuracy) {
        String geoHashBinarty = combineBinary(latitudeBinary, longitudeBinary);
        StringBuffer geoHash = new StringBuffer();
        int count = geoHashBinarty.length() / accuracy;
        int index;
        for (int i = 0; (i + count) <= geoHashBinarty.length(); i = i + count) {
            index = toInteger(geoHashBinarty.substring(i, i + count));
            geoHash.append(_base32[index]);
        }
        return geoHash.toString();
    }
    private static String combineBinary(String latitudeBinary,
                                        String longitudeBinary) {
        StringBuffer binarys = new StringBuffer();
        String lastBinary = "";
        // default length is latitudeBinary.length
        int length = latitudeBinary.length();
        // get longer binary
        if (latitudeBinary.length() < longitudeBinary.length()) {
            lastBinary = longitudeBinary.substring(length,
                    longitudeBinary.length());
        }
        int index = 0;
        for (index = 0; index < length; index++) {
            binarys.append(longitudeBinary.charAt(index));
            binarys.append(latitudeBinary.charAt(index));
        }
        binarys.append(lastBinary);
        return binarys.toString();
    }
    public static String binaryAdd(String a, String b) {
        return toBinaryString(toInteger(a) + toInteger(b));
    }
    /**
     * 10 to binary
     *
     * @param i
     * @return
     */
    public static String toBinaryString(int i) {
        return Integer.toBinaryString(i);
    }
    /**
     * binary to 10
     *
     * @param binary
     * @return
     */
    public static int toInteger(String binary) {
        char[] c = binary.toCharArray();
        int sum = 0;
        for (int i = 0; i < c.length; i++) {
            sum += Integer.parseInt(new String(c, i, 1))
                    * Math.pow(2, c.length - i - 1);
        }
        return sum;
    }
    /**
     *
     * @param hexString
     * @param accuracy
     * @return binary string
     */
    public static String toBinaryString(String hexString, int accuracy) {
        if (hexString == null) {
            return null;
        }
        String bString = "";
        String tmp;
        for (int i = 0; i < accuracy; i++) {
            tmp = "00000" + Integer
                    .toBinaryString(_decodemap.get(hexString.charAt(i)));
            bString += tmp.substring(tmp.length() - 5);
        }
        return bString;
    }
    /**
     *
     * @param binaryString
     * @return [0] 纬度 Latitude 从左到右数双数（从1开始）, [1] 经度 Longitude 从左到右数单数（从0开始）
     */
    public static String[] getBinaryLocation(String binaryString) {
        StringBuffer latitudeString = new StringBuffer();
        StringBuffer longitudeString = new StringBuffer();
        for (int i = 0; i < binaryString.length(); i++) {
            if (i % 2 == 0) {
                longitudeString.append(binaryString.charAt(i));
            } else {
                latitudeString.append(binaryString.charAt(i));
            }
        }
        return new String[] { latitudeString.toString(),
                longitudeString.toString() };
    }
    /**
     *
     * @param binaryString
     * @return 纬度 Latitude 从左到右数双数（从1开始）
     */
    public static String getLatitudeBinary(String binaryString) {
        StringBuffer latitudeString = new StringBuffer();
        for (int i = 1; i < binaryString.length(); i = i + 2) {
            latitudeString.append(binaryString.charAt(i));
        }
        return latitudeString.toString();
    }
    /**
     *
     * @param json : For example: [{\"lat\":43.850679,\"lng\":125.2614},{\"lat\":43.8248,\"lng\":125.222261},{\"lat\":43.810556,\"lng\":125.247323},{\"lat\":43.831488,\"lng\":125.281312}]
     * @return list [latitude1, longitude1, latitude2, longitude2, ...]
     */
    public static List<Double> formatJsonArray(String json) {
        if (json == null || "".equals(json.trim())) {
            return null;
        }
        JSONArray array = null;
        try {
            array = new JSONArray(json);
        } catch (JSONException e) {
            // json array format error
        }
        List<Double> vertex = new ArrayList<Double>();
        JSONObject point = null;
        Double lat = null;
        Double lng = null;
        if (array != null) {
            for (int i = 0; i < array.length(); i++) {
                try {
                    point = array.getJSONObject(i);
                } catch (JSONException e) {
                    // json format error
                    continue;
                }
                if (point != null) {
                    try {
                        lat = Double.valueOf(point.get(LAT).toString());
                        lng = Double.valueOf(point.get(LNG).toString());
                    } catch (Exception e) {
                        // error get json object
                        continue;
                    }
                    if (lat != null && lat != 0 && lng != null && lng != 0) {
                        vertex.add(lat);
                        vertex.add(lng);
                    }
                }
            }
        }
        return vertex;
    }
    /**
     * get max min longitude latitude
     * @param vertex latitude longitude list, [latitude1, longitude1, latitude2, longitude2, ...]
     * @return list [minLatitude,minLongitude,  maxLatitude,maxLongitude]
     */
    public static List<Double> getRect(List<Double> vertex) {
        if (vertex == null || vertex.size() == 0) {
            return null;
        }
        Double lat = null;
        Double lng = null;
        // get max min longitude latitude
        Double minLat = null;
        Double minLng = null;
        Double maxLat = null;
        Double maxLng = null;
        for (int i = 0; i < vertex.size(); i++) {
            if (i % 2 == 0) {
                lat = vertex.get(i);
                if (minLat == null || minLat > lat) {
                    minLat = lat;
                }
                if (maxLat == null || maxLat < lat) {
                    maxLat = lat;
                }
            } else {
                lng = vertex.get(i);
                if (minLng == null || minLng > lng) {
                    minLng = lng;
                }
                if (maxLng == null || maxLng < lng) {
                    maxLng = lng;
                }
            }
        }
        List<Double> rect = new ArrayList<Double>();
        rect.add(minLat);
        rect.add(minLng);
        rect.add(maxLat);
        rect.add(maxLng);
        return rect;
    }
    public static void main(String[] args) {
        List<Double> rect = new ArrayList<Double>();
        rect.add(39.94437424085707);
        rect.add(116.35592895551315);
        rect.add(39.9525495252191);
        rect.add(116.37178578741235);
        // getGeoHasshList(rect);
        // String[] binarys = getBinaryLocation(toBinaryString("wtw3dju121f4",
        // 7));
        // System.out.println(binarys[0] );
        // System.out.println(binarys[1] );
        // 10101100011010111
        // 110101100101000100
        // System.out.println(getGeoHash("10101100011010111",
        // "110101100101000100", DEFAULT_ACCURACY));
        System.out.println(GeoHash.decode_exactly("wtw3dju121f4")[0]);
        System.out.println(GeoHash.decode_exactly("wtw3dju")[0]);
        List<Double> vertex = new ArrayList<Double>();
        vertex.add(39.9525495252191);
        vertex.add(116.3714854014636);
        vertex.add(39.949021300516115);
        vertex.add(116.37178578741235);
        vertex.add(39.948322346161);
        vertex.add(116.36925376064139);
        vertex.add(39.94618381740439);
        vertex.add(116.36312782134115);
        vertex.add(39.94437424085707);
        vertex.add(116.3567443899755);
        vertex.add(39.9482317057509);
        vertex.add(116.35674436851458);
        vertex.add(39.951727072897796);
        vertex.add(116.35592895551315);
        List<String> geoHashList = getGeoHashList(vertex, rect, 5);
        System.out.println(geoHashList.size());
        double[] locations;
        for (String geoHash : geoHashList) {
            locations = GeoHash.decode_exactly(geoHash);
            System.out.println(
                    geoHash + " : " + locations[0] + "  " + locations[1]);
        }
    }
}
