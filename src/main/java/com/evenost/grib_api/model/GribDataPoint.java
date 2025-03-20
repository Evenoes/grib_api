package com.evenost.grib_api.model;

public class GribDataPoint {
    private double latitude;
    private double longitude;
    private double value;
    private long timestamp;
    
    // Constructor with 3 parameters excluding timestamp
    public GribDataPoint(double latitude, double longitude, double value) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.value = value;
        this.timestamp = System.currentTimeMillis();
    }
    
    // Constructor with 4 parameters including timestamp
    public GribDataPoint(double latitude, double longitude, double value, long timestamp) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    // Getters and setters
    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    
    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    
    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}