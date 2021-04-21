package com.presto.udfs.exception;

public class IpConvertRegionException extends RuntimeException{

    private String ip;
    private String msg;

    public IpConvertRegionException(String ip, String msg){
        super(String.format("IpConvertRegionException [%s | %s]", ip, msg));
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public static void main(String[] args) {
        if (true) {
            throw new IpConvertRegionException("129.1.1.1", "convert error");
        }
        System.out.println("after throw exception");
    }
}
