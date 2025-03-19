package com.evenost.grib_api.model;

import java.util.List;

public class GribResponse {
    private List<GribDataPoint> data;
    private double minValue;
    private double maxValue;
    private String parameter;
    
    // Constructor
    public GribResponse(List<GribDataPoint> data, double minValue, double maxValue, String parameter) {
        this.data = data;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.parameter = parameter;
    }
    
    // Getters and setters
    public List<GribDataPoint> getData() { return data; }
    public void setData(List<GribDataPoint> data) { this.data = data; }
    public double getMinValue() { return minValue; }
    public void setMinValue(double minValue) { this.minValue = minValue; }
    public double getMaxValue() { return maxValue; }
    public void setMaxValue(double maxValue) { this.maxValue = maxValue; }
    public String getParameter() { return parameter; }
    public void setParameter(String parameter) { this.parameter = parameter; }
}