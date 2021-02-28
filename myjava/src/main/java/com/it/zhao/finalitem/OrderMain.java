package com.it.zhao.finalitem;

import java.sql.Timestamp;

public class OrderMain {

    private int oid;
    private Timestamp create_time;
    private Double total_money;
    private int status;
    private Timestamp update_time;
    private String uid;
    private String province;

    public OrderMain() {
    }

    public int getOid() {
        return oid;
    }

    public void setOid(int oid) {
        this.oid = oid;
    }

    public Timestamp getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Timestamp create_time) {
        this.create_time = create_time;
    }

    public Double getTotal_money() {
        return total_money;
    }

    public void setTotal_money(Double total_money) {
        this.total_money = total_money;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Timestamp getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Timestamp update_time) {
        this.update_time = update_time;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    @Override
    public String toString() {
        return "OrderMain{" +
                "oid=" + oid +
                ", create_time=" + create_time +
                ", total_money=" + total_money +
                ", status=" + status +
                ", update_time=" + update_time +
                ", uid='" + uid + '\'' +
                ", province='" + province + '\'' +
                '}';
    }

    public OrderMain(int oid, Timestamp create_time, Double total_money, int status, Timestamp update_time, String uid, String province) {
        this.oid = oid;
        this.create_time = create_time;
        this.total_money = total_money;
        this.status = status;
        this.update_time = update_time;
        this.uid = uid;
        this.province = province;
    }
}
