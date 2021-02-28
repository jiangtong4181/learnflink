package com.it.zhao.dimitem;

public class LogBean {
    public String oid;
    public int cid;
    public Double money;
    public Double longtitude;
    public Double latitude;
    public String name;
    public String province;
    public String city;

    @Override
    public String toString() {
        return "LogBean{" +
                "oid='" + oid + '\'' +
                ", cid=" + cid +
                ", money=" + money +
                ", longtitude=" + longtitude +
                ", latitude=" + latitude +
                ", name='" + name + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
