package com.presto.udfs.exception;

public class PhoneNormalizeException extends RuntimeException{

    private String phone;
    private String msg;

    public PhoneNormalizeException(String phone, String msg){
        super(String.format("PhoneNormalizeException [%s | %s]", phone, msg));
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

}
