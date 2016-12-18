package com.elasticplayground.client;

/**
 * Created by luosai on 2016/12/18.
 */
public class User {

    private long id;// id
    private String name;// 姓名
    private double lat;// 纬度
    private double lon;// 经度
    private double[] location;// hashcode

    public User(long id, String name, double lat, double lon) {
        super();
        this.id = id;
        this.name = name;
        this.lat = lat;
        this.lon = lon;
    }
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public double getLat() {
        return lat;
    }
    public void setLat(double lat) {
        this.lat = lat;
    }
    public double getLon() {
        return lon;
    }
    public void setLon(double lon) {
        this.lon = lon;
    }
    public double[] getLocation() {
        return location;
    }
    public void setLocation(double[] location) {
        this.location = location;
    }
}