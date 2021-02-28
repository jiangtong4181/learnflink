package com.it.zhao.finalitem;

import java.sql.Timestamp;

public class OrderDetail {
    private int id;
    private int order_id;
    private int category_id;
    private int sku;
    private Double money;
    private int amount;
    private Timestamp create_time;
    private Timestamp update_time;

    public OrderDetail() {
    }

    public OrderDetail(int id, int order_id, int category_id, int sku, Double money, int amount, Timestamp create_time, Timestamp update_time) {
        this.id = id;
        this.order_id = order_id;
        this.category_id = category_id;
        this.sku = sku;
        this.money = money;
        this.amount = amount;
        this.create_time = create_time;
        this.update_time = update_time;
    }

    public int getId() {
        return id;
    }

    public int getOrder_id() {
        return order_id;
    }

    public int getCategory_id() {
        return category_id;
    }

    public int getSku() {
        return sku;
    }

    public Double getMoney() {
        return money;
    }

    public int getAmount() {
        return amount;
    }

    public Timestamp getCreate_time() {
        return create_time;
    }

    public Timestamp getUpdate_time() {
        return update_time;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public void setSku(int sku) {
        this.sku = sku;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setCreate_time(Timestamp create_time) {
        this.create_time = create_time;
    }

    public void setUpdate_time(Timestamp update_time) {
        this.update_time = update_time;
    }

    @Override
    public String toString() {
        return "OrderDetail{" +
                "id=" + id +
                ", order_id=" + order_id +
                ", category_id=" + category_id +
                ", sku=" + sku +
                ", money=" + money +
                ", amount=" + amount +
                ", create_time=" + create_time +
                ", update_time=" + update_time +
                '}';
    }
}
