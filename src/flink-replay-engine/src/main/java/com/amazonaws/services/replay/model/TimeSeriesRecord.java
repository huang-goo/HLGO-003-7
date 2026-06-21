package com.amazonaws.services.replay.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TimeSeriesRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private long timestamp;
    private Map<String, Double> measures;
    private Map<String, String> dimensions;

    public TimeSeriesRecord() {
        this.measures = new HashMap<>();
        this.dimensions = new HashMap<>();
    }

    public TimeSeriesRecord(long timestamp) {
        this();
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Double> getMeasures() {
        return measures;
    }

    public void setMeasures(Map<String, Double> measures) {
        this.measures = measures;
    }

    public void addMeasure(String name, double value) {
        this.measures.put(name, value);
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

    public double[] getValuesAsArray() {
        double[] values = new double[measures.size()];
        int i = 0;
        for (double v : measures.values()) {
            values[i++] = v;
        }
        return values;
    }

    public int getDimensionCount() {
        return measures.size();
    }

    public boolean hasAllMeasures(String... measureNames) {
        for (String name : measureNames) {
            if (!measures.containsKey(name)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "TimeSeriesRecord{" +
                "timestamp=" + timestamp +
                ", measures=" + measures.size() + " dims" +
                '}';
    }
}
