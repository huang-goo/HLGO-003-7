package com.amazonaws.services.replay.core;

import com.amazonaws.services.replay.model.ReplayJobRequest;
import com.amazonaws.services.replay.model.ReplayJobResult;
import com.amazonaws.services.replay.model.ReplayPoint;
import com.amazonaws.services.replay.model.TimeSeriesRecord;
import com.amazonaws.services.replay.operator.DirtyRecordFilter;
import com.amazonaws.services.replay.operator.RcfDetectOperator;
import com.amazonaws.services.replay.sink.ResultCollectSink;
import com.amazonaws.services.replay.source.TimeSeriesReplaySource;
import com.amazonaws.services.replay.source.TimestreamDataFetcher;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ReplayEngine {
    private static final Logger logger = LoggerFactory.getLogger(ReplayEngine.class);

    private static ReplayEngine instance;
    private final Map<String, ReplayJobResult> jobResults;
    private final Map<String, Thread> jobThreads;
    private final SnapshotManager snapshotManager;
    private final String baseStateDir;

    private ReplayEngine(String baseStateDir) {
        this.baseStateDir = baseStateDir;
        this.jobResults = new ConcurrentHashMap<>();
        this.jobThreads = new ConcurrentHashMap<>();
        this.snapshotManager = new SnapshotManager(baseStateDir);
    }

    public static synchronized ReplayEngine getInstance(String baseStateDir) {
        if (instance == null) {
            instance = new ReplayEngine(baseStateDir);
        }
        return instance;
    }

    public static synchronized ReplayEngine getInstance() {
        return getInstance("./state");
    }

    public ReplayJobResult startReplayJob(ReplayJobRequest request) {
        String jobId = request.getJobId() != null ? request.getJobId() :
                "replay-" + UUID.randomUUID().toString().substring(0, 8);
        request.setJobId(jobId);

        ReplayJobResult result = new ReplayJobResult();
        result.setJobId(jobId);
        result.setStatus("RUNNING");
        result.setStartTime(System.currentTimeMillis());
        jobResults.put(jobId, result);

        Thread jobThread = new Thread(() -> runReplayJob(request, result), "replay-job-" + jobId);
        jobThread.setDaemon(true);
        jobThread.start();
        jobThreads.put(jobId, jobThread);

        logger.info("Started replay job: {}", jobId);
        return result;
    }

    private void runReplayJob(ReplayJobRequest request, ReplayJobResult result) {
        try {
            TimestreamDataFetcher fetcher = new TimestreamDataFetcher(
                    request.getRegion(),
                    request.getDatabaseName(),
                    request.getTableName()
            );

            try {
                List<ReplayPoint> points = fetcher.fetchData(
                        request.getStartTime(),
                        request.getEndTime(),
                        request.getMeasureNames()
                );

                result.setTotalRecords(points.size());
                logger.info("Fetched {} records for job {}", points.size(), request.getJobId());

                if (points.isEmpty()) {
                    result.setStatus("COMPLETED");
                    result.setEndTime(System.currentTimeMillis());
                    return;
                }

                executeFlinkJob(request, result, points);

            } finally {
                fetcher.close();
            }

        } catch (Exception e) {
            logger.error("Replay job {} failed", request.getJobId(), e);
            result.setStatus("FAILED");
            result.setErrorMessage(e.getMessage());
            result.setEndTime(System.currentTimeMillis());
        }
    }

    @SuppressWarnings("unchecked")
    private void executeFlinkJob(ReplayJobRequest request, ReplayJobResult result,
                                 List<ReplayPoint> points) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(10, TimeUnit.SECONDS)
        ));
        env.setParallelism(1);

        if (request.isEnableCheckpoint()) {
            SnapshotManager snapMgr = new SnapshotManager(
                    baseStateDir + "/" + request.getJobId(),
                    request.getCheckpointInterval(),
                    CheckpointingMode.EXACTLY_ONCE
            );
            snapMgr.configureCheckpointing(env);
        }

        DataStream<TimeSeriesRecord> sourceStream = env.addSource(
                new TimeSeriesReplaySource(points, request.getSpeedFactor())
        ).name("TimeSeriesReplaySource");

        SingleOutputStreamOperator<TimeSeriesRecord> filteredStream;
        if (request.isEnableDirtyDataIsolation()) {
            filteredStream = sourceStream
                    .process(new DirtyRecordFilter(request.getDirtyDataThreshold()))
                    .name("DirtyRecordFilter");

            filteredStream.getSideOutput(DirtyRecordFilter.DIRTY_DATA_TAG)
                    .addSink(new ResultCollectSink<ReplayJobResult.DirtyDataRecord>(request.getJobId(), "dirty"))
                    .name("DirtyDataSink");
        } else {
            filteredStream = (SingleOutputStreamOperator<TimeSeriesRecord>) sourceStream;
        }

        ReplayJobRequest.RcfParams rcfParams = request.getRcfParams();
        if (rcfParams == null) {
            rcfParams = new ReplayJobRequest.RcfParams();
        }

        DataStream<ReplayJobResult.AnomalyResult> anomalyStream = filteredStream
                .keyBy(record -> record.getDimensions().getOrDefault("measure_group", "default"))
                .process(new RcfDetectOperator(
                        rcfParams.getShingleSize(),
                        rcfParams.isShingleCyclic(),
                        rcfParams.getNumberOfTrees(),
                        rcfParams.getSampleSize(),
                        rcfParams.getLambda(),
                        rcfParams.getRandomSeed(),
                        1.0
                )).name("RcfDetectOperator");

        anomalyStream
                .addSink(new ResultCollectSink<ReplayJobResult.AnomalyResult>(request.getJobId(), "anomaly"))
                .name("AnomalyResultSink");

        result.setStatus("RUNNING");
        logger.info("Executing Flink job: {}", request.getJobId());

        env.execute("ReplayJob-" + request.getJobId());

        List<ReplayJobResult.AnomalyResult> anomalyResults =
                ResultCollectSink.getResults(request.getJobId(), "anomaly");
        List<ReplayJobResult.DirtyDataRecord> dirtyRecords =
                ResultCollectSink.getResults(request.getJobId(), "dirty");

        result.setAnomalyResults(anomalyResults);
        result.setDirtyDataRecords(dirtyRecords);
        result.setProcessedRecords(anomalyResults.size());
        result.setDirtyRecords(dirtyRecords.size());

        if (request.isEnableCheckpoint()) {
            String savepointPath = snapshotManager.createSavepointPath(request.getJobId());
            result.setSavepointPath(savepointPath);
        }

        result.setStatus("COMPLETED");
        result.setEndTime(System.currentTimeMillis());

        logger.info("Flink job {} completed. Anomalies: {}, Dirty records: {}",
                request.getJobId(), anomalyResults.size(), dirtyRecords.size());
    }

    public ReplayJobResult getJobResult(String jobId) {
        return jobResults.get(jobId);
    }

    public List<ReplayJobResult.AnomalyResult> getAnomalyResults(String jobId) {
        return ResultCollectSink.getResults(jobId, "anomaly");
    }

    public List<ReplayJobResult.DirtyDataRecord> getDirtyRecords(String jobId) {
        return ResultCollectSink.getResults(jobId, "dirty");
    }

    public boolean stopJob(String jobId) {
        Thread thread = jobThreads.get(jobId);
        if (thread != null && thread.isAlive()) {
            thread.interrupt();
            ReplayJobResult result = jobResults.get(jobId);
            if (result != null) {
                result.setStatus("STOPPED");
                result.setEndTime(System.currentTimeMillis());
            }
            logger.info("Stopped replay job: {}", jobId);
            return true;
        }
        return false;
    }

    public boolean cleanupJob(String jobId) {
        stopJob(jobId);
        jobResults.remove(jobId);
        jobThreads.remove(jobId);
        ResultCollectSink.clearJobResults(jobId);
        snapshotManager.cleanupOldCheckpoints(jobId);
        logger.info("Cleaned up replay job: {}", jobId);
        return true;
    }

    public SnapshotManager getSnapshotManager() {
        return snapshotManager;
    }
}
