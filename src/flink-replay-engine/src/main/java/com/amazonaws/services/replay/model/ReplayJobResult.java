package com.amazonaws.services.replay.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReplayJobResult implements Serializable {
    private static final long serialVersionUID = 1L;
    private String jobId;
    private String status;
    private long startTime;
    private long endTime;
    private long totalRecords;
    private long processedRecords;
    private long dirtyRecords;
    private String errorMessage;
    private List<AnomalyResult> anomalyResults;
    private List<DirtyDataRecord> dirtyDataRecords;
    private String savepointPath;

    public ReplayJobResult() {
        this.anomalyResults = new ArrayList<>();
        this.dirtyDataRecords = new ArrayList<>();
        this.status = "INITIALIZING";
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getTotalRecords() {
        return totalRecords;
    }

    public void setTotalRecords(long totalRecords) {
        this.totalRecords = totalRecords;
    }

    public long getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(long processedRecords) {
        this.processedRecords = processedRecords;
    }

    public long getDirtyRecords() {
        return dirtyRecords;
    }

    public void setDirtyRecords(long dirtyRecords) {
        this.dirtyRecords = dirtyRecords;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public List<AnomalyResult> getAnomalyResults() {
        return anomalyResults;
    }

    public void setAnomalyResults(List<AnomalyResult> anomalyResults) {
        this.anomalyResults = anomalyResults;
    }

    public List<DirtyDataRecord> getDirtyDataRecords() {
        return dirtyDataRecords;
    }

    public void setDirtyDataRecords(List<DirtyDataRecord> dirtyDataRecords) {
        this.dirtyDataRecords = dirtyDataRecords;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    public static class AnomalyResult implements Serializable {
        private static final long serialVersionUID = 1L;
        private long timestamp;
        private String measureGroup;
        private double anomalyScore;
        private boolean isAnomaly;
        private double threshold;

        public AnomalyResult() {}

        public AnomalyResult(long timestamp, String measureGroup, double anomalyScore, boolean isAnomaly, double threshold) {
            this.timestamp = timestamp;
            this.measureGroup = measureGroup;
            this.anomalyScore = anomalyScore;
            this.isAnomaly = isAnomaly;
            this.threshold = threshold;
        }

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

        public double getAnomalyScore() {
            return anomalyScore;
        }

        public void setAnomalyScore(double anomalyScore) {
            this.anomalyScore = anomalyScore;
        }

        public boolean isAnomaly() {
            return isAnomaly;
        }

        public void setAnomaly(boolean anomaly) {
            isAnomaly = anomaly;
        }

        public double getThreshold() {
            return threshold;
        }

        public void setThreshold(double threshold) {
            this.threshold = threshold;
        }
    }

    public static class DirtyDataRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        private long timestamp;
        private String measureName;
        private String value;
        private String reason;

        public DirtyDataRecord() {}

        public DirtyDataRecord(long timestamp, String measureName, String value, String reason) {
            this.timestamp = timestamp;
            this.measureName = measureName;
            this.value = value;
            this.reason = reason;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public String getMeasureName() {
            return measureName;
        }

        public void setMeasureName(String measureName) {
            this.measureName = measureName;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
