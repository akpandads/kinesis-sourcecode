package com.akpanda.kinesis.domain;

import com.google.gson.Gson;

public class VirtualLocation {
    private String vlat;
    private String vlong;

    private static Gson gson = new Gson();

    public String getVlat() {
        return vlat;
    }

    public void setVlat(String vlat) {
        this.vlat = vlat;
    }

    public String getVlong() {
        return vlong;
    }

    public void setVlong(String vlong) {
        this.vlong = vlong;
    }
    public VirtualLocation(){
        this.vlong = String.valueOf(Math.random() * Math.PI * 2);
        this.vlat = String.valueOf(Math.acos(Math.random() * 2 - 1));
    }

    public String virLocationToJson(){
        String result = gson.toJson(this);
        return result;
    }
}
