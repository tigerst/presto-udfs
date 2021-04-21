package com.presto.tools.gis;

import java.util.ArrayList;
import java.util.List;
import com.presto.tools.UDFConstant;
public class GISCross {

    public static List<Point> getRect(List<Point> vertex){
        if(vertex.size()<1){
            return null;
        }
        double minX=vertex.get(0).x;
        double minY=vertex.get(0).y;
        double maxX=vertex.get(0).x;
        double maxY=vertex.get(0).y;
        for(int i=1;i<vertex.size();i++){
            if(vertex.get(i).x>maxX){
                maxX=vertex.get(i).x;
            }
            if(vertex.get(i).x<minX){
                minX=vertex.get(i).x;
            }
            if(vertex.get(i).y>maxY){
                maxY=vertex.get(i).y;
            }
            if(vertex.get(i).y<minY){
                minY=vertex.get(i).y;
            }
        }
        List<Point> res =new ArrayList<Point>();
        res.add(new Point(minX,minY));
        res.add(new Point(maxX, maxY));
        return res;
    }


    public static int isInner(Point p, List<Point> vertex){
        if(vertex.size()<3){
            return UDFConstant.GIS_ERROR_REGION;
        }
        if(vertex.size()>50){
            List<Point> rect= getRect(vertex);
            if(p.x<rect.get(0).x || p.x>rect.get(1).x ||
                    p.y<rect.get(0).y ||p.y>rect.get(1).y){
                return UDFConstant.GIS_INTERSECT_OUT;
            }
        }

        int regionFlag=0;
        int i;
        for(i=0;i<vertex.size();i++){
            if(vertex.get(i).x!=p.x){
                regionFlag=vertex.get(i).x<p.x? -1:1;
                break;
            }
        }

        int sum=0;
        int sIndex=i;

        for(int j=1;j<vertex.size()+1;j++){ //起点已判断
            int index = (i+j)<vertex.size()? (i+j):(i+j-vertex.size());
            if(vertex.get(index).x==p.x){
                if((vertex.get(index).x==vertex.get(sIndex).x)
                        && (vertex.get(index).y-p.y)*(vertex.get(sIndex).y-p.y)<=0){
                    return UDFConstant.GIS_INTERSECT_BOUND;
                }

                if(vertex.get(index).y==p.y){
                    return UDFConstant.GIS_INTERSECT_BOUND;
                }
            } else if((vertex.get(index).x>p.x && regionFlag==-1)
                    ||(vertex.get(index).x<p.x && regionFlag==1)){
                regionFlag= -regionFlag;
                double k= (vertex.get(index).y-vertex.get(sIndex).y)/(vertex.get(index).x-vertex.get(sIndex).x);
                double y= vertex.get(sIndex).y+ k * (p.x-vertex.get(sIndex).x);
                if(y==p.y){
                    return UDFConstant.GIS_INTERSECT_BOUND;
                }
                if(y>p.y){
                    sum+=1;
                }
            }
            sIndex=index;
            continue;
        }
        return ((sum&1)==1)? UDFConstant.GIS_INTERSECT_INNER:UDFConstant.GIS_INTERSECT_OUT;
    }


    public static int isInner(Point p, List<Double> vertex, List<Double> rect){
        if(vertex.size()<6){
            return UDFConstant.GIS_ERROR_REGION;
        }
        if(rect.size()<4){
            return UDFConstant.GIS_ERROR_REGION;
        }

        if(p.x<rect.get(0) || p.x>rect.get(2) ||
                p.y<rect.get(1) ||p.y>rect.get(3)){
            return UDFConstant.GIS_INTERSECT_OUT;
        }
        int regionFlag=0;
        int i;
        for(i=0;i<vertex.size()-1;i+=2){
            if(vertex.get(i).doubleValue()!=p.x){
                regionFlag=vertex.get(i)<p.x? -1:1;
                break;
            }
        }

        int sum=0;
        int sIndex=i;

        for(int j=2;j<vertex.size()+2;j+=2){ //起点已判断
            int index = (i+j)<vertex.size()? (i+j):(i+j-vertex.size());
            if(vertex.get(index).doubleValue()==p.x){
                if((vertex.get(index).doubleValue()==vertex.get(sIndex).doubleValue())
                        && (vertex.get(index+1)-p.y)*(vertex.get(sIndex+1)-p.y)<=0){
                    return UDFConstant.GIS_INTERSECT_BOUND;
                }

                if(vertex.get(index+1).doubleValue()==p.y){
                    return UDFConstant.GIS_INTERSECT_BOUND;
                }
            }else if((vertex.get(index)>p.x && regionFlag==-1)
                    ||(vertex.get(index)<p.x && regionFlag==1)){
                regionFlag= -regionFlag;
                double k= (vertex.get(index+1)-vertex.get(sIndex+1))/(vertex.get(index)-vertex.get(sIndex));
                double y= vertex.get(sIndex+1)+ k * (p.x-vertex.get(sIndex));
                if(y==p.y){
                    return UDFConstant.GIS_INTERSECT_BOUND;
                }
                if(y>p.y){
                    sum+=1;
                }
            }
            sIndex=index;
            continue;
        }
        return ((sum&1)==1)? UDFConstant.GIS_INTERSECT_INNER:UDFConstant.GIS_INTERSECT_OUT;
    }








    public static void main(String[] argc){

        List<Point> vertex= new ArrayList<Point>();
        vertex.add(new Point(1, 1));
        vertex.add(new Point(2, 3));
        vertex.add(new Point(2, 4));
        vertex.add(new Point(3, 1));
        vertex.add(new Point(4, 1));
        vertex.add(new Point(2, 0));

        Point p1=new Point(5,1);
        Point p2=new Point(2,2.5);
        Point p3=new Point(1,3.5);
        Point p4=new Point(-2, 3);

        Point p5=new Point(2,3.5);
        Point p6=new Point(3.5,1.1);
        Point p7=new Point(3.2,0.7);
        Point p8=new Point(1.5, 1.3);

        System.out.println("Test isInner simple version.....");
        System.out.println("P1 外部点: "+ (isInner(p1,vertex)==0));
        System.out.println("P2 内部点: "+ (isInner(p2,vertex)==1));
        System.out.println("P3 外部点: "+ (isInner(p3,vertex)==0));
        System.out.println("P4 外部点: "+ (isInner(p4,vertex)==0));
        System.out.println("P5 边界点: "+ (isInner(p5,vertex)==-1));
        System.out.println("P6 外部点: "+ (isInner(p6,vertex)==0));

        System.out.println("P7 内部点: "+ (isInner(p7,vertex)==1));
        System.out.println("P8 内部点: "+ (isInner(p8,vertex)==1));

    }




}
