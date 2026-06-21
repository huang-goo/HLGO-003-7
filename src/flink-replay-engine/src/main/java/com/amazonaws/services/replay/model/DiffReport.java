package com.amazonaws.services.replay.model;

import java.util.ArrayList;
import java.util.List;

public class DiffReport {
    private String jobId;
    private String baselineJobId;
    private long generatedTime;
    private long totalComparedRecords;
    private double avgScoreDiff;
    private double maxScoreDiff;
    private double minScoreDiff;
    private int anomalyCountDiff;
    private double precisionDiff;
    private double recallDiff;
    private List<DiffRecord> diffRecords;
    private List<String> insights;

    public DiffReport() {
        this.diffRecords = new ArrayList<>();
        this.insights = new ArrayList<>();
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getBaselineJobId() {
        return baselineJobId;
    }

    public void setBaselineJobId(String baselineJobId) {
        this.baselineJobId = baselineJobId;
    }

    public long getGeneratedTime() {
        return generatedTime;
    }

    public void setGeneratedTime(long generatedTime) {
        this.generatedTime = generatedTime;
    }

    public long getTotalComparedRecords() {
        return totalComparedRecords;
    }

    public void setTotalComparedRecords(long totalComparedRecords) {
        this.totalComparedRecords = totalComparedRecords;
    }

    public double getAvgScoreDiff() {
        return avgScoreDiff;
    }

    public void setAvgScoreDiff(double avgScoreDiff) {
        this.avgScoreDiff = avgScoreDiff;
    }

    public double getMaxScoreDiff() {
        return maxScoreDiff;
    }

    public void setMaxScoreDiff(double maxScoreDiff) {
        this.maxScoreDiff = maxScoreDiff;
    }

    public double getMinScoreDiff() {
        return minScoreDiff;
    }

    public void setMinScoreDiff(double minScoreDiff) {
        this.minScoreDiff = minScoreDiff;
    }

    public int getAnomalyCountDiff() {
        return anomalyCountDiff;
    }

    public void setAnomalyCountDiff(int anomalyCountDiff) {
        this.anomalyCountDiff = anomalyCountDiff;
    }

    public double getPrecisionDiff() {
        return precisionDiff;
    }

    public void setPrecisionDiff(double precisionDiff) {
        this.precisionDiff = precisionDiff;
    }

    public double getRecallDiff() {
        return recallDiff;
    }

    public void setRecallDiff(double recallDiff) {
        this.recallDiff = recallDiff;
    }

    public List<DiffRecord> getDiffRecords() {
        return diffRecords;
    }

    public void setDiffRecords(List<DiffRecord> diffRecords) {
        this.diffRecords = diffRecords;
    }

    public List<String> getInsights() {
        return insights;
    }

    public void setInsights(List<String> insights) {
        this.insights = insights;
    }

    public static class DiffRecord {
        private long timestamp;
        private String measureGroup;
        private double baselineScore;
        private double currentScore;
        private double scoreDiff;
        private boolean baselineIsAnomaly;
        private boolean currentIsAnomaly;
        private String diffType;

        public DiffRecord() {}

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getMeasureGroup() {
            return measureGroup;
        }

        public void setMeasureGroup(String measureGroup) {
            this.measureGroup = measureGroup;
        }

        public double getBaselineScore() {
            return baselineScore;
        }

        public void setBaselineScore(double baselineScore) {
            this.baselineScore = baselineScore;
        }

        public double getCurrentScore() {
            return currentScore;
        }

        public void setCurrentScore(double currentScore) {
            this.currentScore = currentScore;
        }

        public double getScoreDiff() {
            return scoreDiff;
        }

        public void setScoreDiff(double scoreDiff) {
            this.scoreDiff = scoreDiff;
        }

        public boolean isBaselineIsAnomaly() {
            return baselineIsAnomaly;
        }

        public void setBaselineIsAnomaly(boolean baselineIsAnomaly) {
            this.baselineIsAnomaly = baselineIsAnomaly;
        }

        public boolean isCurrentIsAnomaly() {
            return currentIsAnomaly;
        }

        public void setCurrentIsAnomaly(boolean currentIsAnomaly) {
            this.currentIsAnomaly = currentIsAnomaly;
        }

        public String getDiffType() {
            return diffType;
        }

        public void setDiffType(String diffType) {
            this.diffType = diffType;
        }
    }
}
