package com.presto.tools.gis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GISCover {

    private Point start= null;
    private int geohashLength=0;
    private double latGap=0L;
    private double lngGap=0L;
    private boolean isSort=false;

    private Map<Integer, List<Double>> joint=new HashMap<Integer, List<Double>>();

    public GISCover(String startGeohash){
        Point st=Point.fromGeoHash(startGeohash);
        geohashLength=startGeohash.length();
        latGap=180/Math.pow(2, Math.floor(2.5*geohashLength));
        lngGap=360/Math.pow(2, Math.ceil(2.5*geohashLength));
        start=new Point(st.x-latGap/2, st.y-lngGap/2); //网格起点
    }

    private Point getGridCoordinates(Point p){
        return new Point((p.x-start.x)/latGap, (p.y-start.y)/lngGap);
    }

    private int centerGridLat(double lat){
        return (int)(0.5+ (lat-start.x)/latGap);
    }


    public void processPolygon(List<Point> vertex){
        if(vertex.size()<3){
            return ;
        }
        Point currentStart=getGridCoordinates(vertex.get(0));
        int latIndex= centerGridLat(vertex.get(0).x);

        for(int i=1;i<vertex.size()+1;i++){ //起点已判断
            int destLatIndex=0;
            Point dest=null;

            if(i==vertex.size()){
                destLatIndex =centerGridLat(vertex.get(0).x);
                dest=getGridCoordinates(vertex.get(0));
            }else{
                destLatIndex =centerGridLat(vertex.get(i).x);
                dest=getGridCoordinates(vertex.get(i));
            }

            if(latIndex==destLatIndex){
                currentStart=dest;
                continue;
            }
            double k=(dest.y-currentStart.y)/(dest.x-currentStart.x);
            double b=dest.y-dest.x*k;


            int minIndex=latIndex<destLatIndex?latIndex:destLatIndex;
            int maxIndex=latIndex<destLatIndex?destLatIndex:latIndex;
            for(int j=minIndex;j<maxIndex; j++){
                if(! joint.containsKey(j)){
                    joint.put(j, new ArrayList<Double>());
                }
                joint.get(j).add(k* (j+0.5)+b);
            }
            currentStart=dest;
            latIndex=destLatIndex;
        }
        isSort=false;
    }



    public void processPolygonVertex(List<Double> vertex){
        List<Point> pts=new ArrayList<Point>();
        for(int i=0;i<vertex.size()-1; i=i+2){
            pts.add(new Point(vertex.get(i), vertex.get(i+1)));
        }
        processPolygon(pts);
    }




    private void sortJoints(){
        for(Integer k:joint.keySet()){
            java.util.Collections.sort(joint.get(k));
        }
        isSort=true;
    }


    public List<String> getGeohashList(){
        if(!isSort){
            sortJoints();
            isSort=true;
        }
        List<String> result=new ArrayList<String>();
        //	log.info("start new geohash List");
        for(Integer key:joint.keySet()){

            double lat=start.x+ (key +0.5)* latGap;
            for(int i=0;i<joint.get(key).size()-1; i=i+2){
                //		System.out.println("size: "+ joint.get(key).size());
                int startInterval=(int) (joint.get(key).get(i)+0.5);
                int endInterval=(int) (joint.get(key).get(i+1)+0.5);
                for(int p=startInterval; p<endInterval; p++){
                    result.add(GeoHash.encode(lat, start.y+(p+0.5)*lngGap, geohashLength));
                }
            }
        }
        return result;
    }


    public double getArea(){
        if(!isSort){
            sortJoints();
            isSort=true;
        }
        double area=0;
        for(Integer key:joint.keySet()){
            double lngSize=0.0;
            double lat=start.x+ (key +0.5)* latGap;
            for(int i=0;i<joint.get(key).size()-1; i=i+2){
                lngSize+= joint.get(key).get(i+1)- joint.get(key).get(i);
            }
            area+= Math.cos(lat/57.29565)* latGap*lngGap*111.111*111.111*lngSize;
        }
        return area;
    }










}