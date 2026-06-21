package com.amazonaws.services.replay.core;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SnapshotManager {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    private final String checkpointDir;
    private final String savepointDir;
    private final long checkpointInterval;
    private final CheckpointingMode checkpointingMode;

    public SnapshotManager(String baseDir) {
        this(baseDir, 60000, CheckpointingMode.EXACTLY_ONCE);
    }

    public SnapshotManager(String baseDir, long checkpointInterval, CheckpointingMode mode) {
        this.checkpointDir = baseDir + "/checkpoints";
        this.savepointDir = baseDir + "/savepoints";
        this.checkpointInterval = checkpointInterval;
        this.checkpointingMode = mode;

        try {
            Files.createDirectories(Paths.get(checkpointDir));
            Files.createDirectories(Paths.get(savepointDir));
        } catch (IOException e) {
            logger.warn("Failed to create checkpoint/savepoint directories", e);
        }
    }

    public void configureCheckpointing(StreamExecutionEnvironment env) {
        env.enableCheckpointing(checkpointInterval, checkpointingMode);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);

        try {
            String backendPath = "file://" + new File(checkpointDir).getAbsolutePath();
            env.setStateBackend(new FsStateBackend(backendPath));
            logger.info("State backend configured: {}", backendPath);
        } catch (Exception e) {
            logger.warn("Failed to set state backend, using default", e);
        }
    }

    public String createSavepointPath(String jobId) {
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String savepointPath = savepointDir + "/" + jobId + "_" + timestamp;
        try {
            Files.createDirectories(Paths.get(savepointPath));
        } catch (IOException e) {
            logger.warn("Failed to create savepoint directory: {}", savepointPath, e);
        }
        return savepointPath;
    }

    public String getCheckpointDir() {
        return checkpointDir;
    }

    public String getSavepointDir() {
        return savepointDir;
    }

    public String resolveSavepointPath(String savepointPath) {
        if (savepointPath == null || savepointPath.isEmpty()) {
            return null;
        }

        File file = new File(savepointPath);
        if (file.isAbsolute()) {
            return savepointPath;
        }

        File resolved = new File(savepointDir, savepointPath);
        if (resolved.exists()) {
            return resolved.getAbsolutePath();
        }

        return null;
    }

    public boolean savepointExists(String savepointPath) {
        if (savepointPath == null) {
            return false;
        }
        File file = new File(savepointPath);
        return file.exists() && file.isDirectory();
    }

    public void cleanupOldCheckpoints(String jobId) {
        File jobCheckpointDir = new File(checkpointDir, jobId);
        if (jobCheckpointDir.exists()) {
            deleteDirectory(jobCheckpointDir);
            logger.info("Cleaned up checkpoints for job: {}", jobId);
        }
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
}
