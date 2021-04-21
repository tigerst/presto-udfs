package com.presto.udfs.scalar.hiveUdfs;

import com.facebook.presto.spi.function.*;
import com.facebook.presto.common.type.StandardTypes;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.presto.tools.gis.GISCover;
import com.presto.tools.gis.CoveredGeohash;
import com.presto.tools.gis.GeoHash;
import com.presto.tools.UDFConstant;

public final class GetGeoArea {

    private GetGeoArea(){

    }

    @ScalarFunction(value = "get_geo_area")
    @SqlNullable
    @SqlType(StandardTypes.DOUBLE)
    public static Double jsonExtractScalar_key_2(@SqlType(StandardTypes.VARCHAR) Slice json, @SqlType(StandardTypes.INTEGER) long accuracy, @SqlType(StandardTypes.INTEGER) long jsonFormat)
    {

        if (json == null || json.toStringUtf8().trim().length()<8) {
            return 0.0;
        }
        if(UDFConstant.POLYGON_FROMAT_POLYGON==jsonFormat){
            return getAreaFromPolygon(json.toStringUtf8(), (int)accuracy);
        }
        if(UDFConstant.POLYGON_FROMAT_GEOJSON==jsonFormat){
            return getAreaFromGeojson(json.toStringUtf8(), (int)accuracy);
        }
        return 0.0;
    }

    private static Double getAreaFromPolygon(String json, int accuracy) {
        try {
            List<Double> vertex = CoveredGeohash.formatJsonArray(json);
            List<Double> rect=CoveredGeohash.getRect(vertex);
            String startGeohash=GeoHash.encode(rect.get(0), rect.get(1), accuracy );

            GISCover gc=new GISCover(startGeohash);
            gc.processPolygonVertex(vertex);
            return gc.getArea();
        } catch (Exception e) {
            // error get geoHash
        }
        return 0.0;
    }


    private static  Double getAreaFromGeojson(String json, int accuracy)  {
        Double area=0.0;

        try {
            JSONObject jobj=new JSONObject(json);
            JSONArray jfeatures=jobj.getJSONArray("features");

            //循环所有独立外圈
            for(int i=0; i<jfeatures.length();i++){
                JSONArray jcoord=jfeatures.getJSONObject(i).getJSONObject("geometry").getJSONArray("coordinates");

                //处理外环
                JSONArray jpts= jcoord.getJSONArray(0);
                List<Double> tempVertex=new ArrayList<Double>();

                for(int k=0;k<jpts.length();k++){
                    tempVertex.add(jpts.getJSONArray(k).getDouble(1));
                    tempVertex.add(jpts.getJSONArray(k).getDouble(0));
                }

                List<Double> rect=CoveredGeohash.getRect(tempVertex);
                String startGeohash=GeoHash.encode(rect.get(0), rect.get(1), accuracy );

                GISCover gc=new GISCover(startGeohash);
                gc.processPolygonVertex(tempVertex);

                //循环其他所有环，此时可以不区分内环还是外环，处理第一个是为了确定外接矩形的位置
                for(int j=1;j<jcoord.length();j++){
                    tempVertex=new ArrayList<Double>();
                    //循环所有点，获取经纬度数据，形成环
                    jpts=jcoord.getJSONArray(j);
                    for(int k=0;k<jpts.length();k++){
                        tempVertex.add(jpts.getJSONArray(k).getDouble(1));
                        tempVertex.add(jpts.getJSONArray(k).getDouble(0));
                    }
                    gc.processPolygonVertex(tempVertex);
                }
                area+=gc.getArea();
            }
        } catch (JSONException e) {
            // error get geoHash
            e.printStackTrace();
        }
        return area;
    }

}
