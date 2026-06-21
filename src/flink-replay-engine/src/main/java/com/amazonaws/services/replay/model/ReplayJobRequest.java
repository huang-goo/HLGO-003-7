package com.amazonaws.services.replay.model;

import java.util.ArrayList;
import java.util.List;

public class ReplayJobRequest {
    private String jobId;
    private String databaseName;
    private String tableName;
    private String region;
    private long startTime;
    private long endTime;
    private double speedFactor = 1.0;
    private String savepointPath;
    private boolean enableCheckpoint = true;
    private long checkpointInterval = 60000;
    private String checkpointDir;
    private boolean enableDirtyDataIsolation = true;
    private double dirtyDataThreshold = 3.0;
    private List<String> measureNames;
    private RcfParams rcfParams;
    private String baselineJobId;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
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

    public double getSpeedFactor() {
        return speedFactor;
    }

    public void setSpeedFactor(double speedFactor) {
        this.speedFactor = speedFactor;
    }

    public String getSavepointPath() {
        return savepointPath;
    }

    public void setSavepointPath(String savepointPath) {
        this.savepointPath = savepointPath;
    }

    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }

    public void setEnableCheckpoint(boolean enableCheckpoint) {
        this.enableCheckpoint = enableCheckpoint;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public void setCheckpointInterval(long checkpointInterval) {
        this.checkpointInterval = checkpointInterval;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public void setCheckpointDir(String checkpointDir) {
        this.checkpointDir = checkpointDir;
    }

    public boolean isEnableDirtyDataIsolation() {
        return enableDirtyDataIsolation;
    }

    public void setEnableDirtyDataIsolation(boolean enableDirtyDataIsolation) {
        this.enableDirtyDataIsolation = enableDirtyDataIsolation;
    }

    public double getDirtyDataThreshold() {
        return dirtyDataThreshold;
    }

    public void setDirtyDataThreshold(double dirtyDataThreshold) {
        this.dirtyDataThreshold = dirtyDataThreshold;
    }

    public List<String> getMeasureNames() {
        return measureNames;
    }

    public void setMeasureNames(List<String> measureNames) {
        this.measureNames = measureNames;
    }

    public RcfParams getRcfParams() {
        return rcfParams;
    }

    public void setRcfParams(RcfParams rcfParams) {
        this.rcfParams = rcfParams;
    }

    public String getBaselineJobId() {
        return baselineJobId;
    }

    public void setBaselineJobId(String baselineJobId) {
        this.baselineJobId = baselineJobId;
    }

    public static class RcfParams {
        private int shingleSize = 1;
        private boolean shingleCyclic = false;
        private int numberOfTrees = 50;
        private int sampleSize = 8192;
        private double lambda = 0.00001220703125;
        private int randomSeed = 42;

        public int getShingleSize() {
            return shingleSize;
        }

        public void setShingleSize(int shingleSize) {
            this.shingleSize = shingleSize;
        }

        public boolean isShingleCyclic() {
            return shingleCyclic;
        }

        public void setShingleCyclic(boolean shingleCyclic) {
            this.shingleCyclic = shingleCyclic;
        }

        public int getNumberOfTrees() {
            return numberOfTrees;
        }

        public void setNumberOfTrees(int numberOfTrees) {
            this.numberOfTrees = numberOfTrees;
        }

        public int getSampleSize() {
            return sampleSize;
        }

        public void setSampleSize(int sampleSize) {
            this.sampleSize = sampleSize;
        }

        public double getLambda() {
            return lambda;
        }

        public void setLambda(double lambda) {
            this.lambda = lambda;
        }

        public int getRandomSeed() {
            return randomSeed;
        }

        public void setRandomSeed(int randomSeed) {
            this.randomSeed = randomSeed;
        }
    }
}
