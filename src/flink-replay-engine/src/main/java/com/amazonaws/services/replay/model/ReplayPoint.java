package com.amazonaws.services.replay.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ReplayPoint implements Serializable {
    private static final long serialVersionUID = 1L;
    private String measureName;
    private String measureValue;
    private String measureValueType;
    private long time;
    private String timeUnit;
    private Map<String, String> dimensions;

    public ReplayPoint() {
        this.dimensions = new HashMap<>();
    }

    public ReplayPoint(String measureName, String measureValue, long time) {
        this();
        this.measureName = measureName;
        this.measureValue = measureValue;
        this.measureValueType = "DOUBLE";
        this.time = time;
        this.timeUnit = "MILLISECONDS";
    }

    public String getMeasureName() {
        return measureName;
    }

    public void setMeasureName(String measureName) {
        this.measureName = measureName;
    }

    public String getMeasureValue() {
        return measureValue;
    }

    public void setMeasureValue(String measureValue) {
        this.measureValue = measureValue;
    }

    public String getMeasureValueType() {
        return measureValueType;
    }

    public void setMeasureValueType(String measureValueType) {
        this.measureValueType = measureValueType;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(String timeUnit) {
        this.timeUnit = timeUnit;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = dimensions;
    }

    public void addDimension(String name, String value) {
        this.dimensions.put(name, value);
    }

    public double getMeasureValueAsDouble() {
        try {
            return Double.parseDouble(measureValue);
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    @Override
    public String toString() {
        return "ReplayPoint{" +
                "measureName='" + measureName + '\'' +
                ", measureValue='" + measureValue + '\'' +
                ", time=" + time +
                ", dimensions=" + dimensions +
                '}';
    }
}
