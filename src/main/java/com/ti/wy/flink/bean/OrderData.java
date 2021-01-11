package com.ti.wy.flink.bean;

/**
 * @author wb.lixinlin
 * @date 2020/12/10
 */
public class OrderData {

    private String order_sn;
    private Double money;
    private String create_time;
    private String update_time;
    private String date_interval;

    public OrderData(String order_sn, Double money, String create_time, String update_time, String date_interval) {
        this.order_sn = order_sn;
        this.money = money;
        this.create_time = create_time;
        this.update_time = update_time;
        this.date_interval = date_interval;
    }

    public OrderData() {
    }

    public String getOrder_sn() {
        return order_sn;
    }

    public void setOrder_sn(String order_sn) {
        this.order_sn = order_sn;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public String getCreate_time() {
        return create_time;
    }

    public void setCreate_time(String create_time) {
        this.create_time = create_time;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }

    public String getDate_interval() {
        return date_interval;
    }

    public void setDate_interval(String date_interval) {
        this.date_interval = date_interval;
    }
}
