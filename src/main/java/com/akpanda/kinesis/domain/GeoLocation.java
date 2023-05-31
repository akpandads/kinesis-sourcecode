package com.akpanda.kinesis.domain;

import com.google.gson.Gson;

import java.nio.ByteBuffer;

public class GeoLocation {
    private String latitude;
    private String longitude;

    private String publisherSource;

    private static Gson gson = new Gson();

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
    public GeoLocation(String publisherSource){
        this.longitude = String.valueOf(Math.random() * Math.PI * 2);
        this.latitude = String.valueOf(Math.acos(Math.random() * 2 - 1));
        this.publisherSource = publisherSource;
    }

    public String geoLocationToJson(){
        String result = gson.toJson(this);
        return result;
    }
}
