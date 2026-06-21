package com.amazonaws.services.replay.demo;

import com.amazonaws.services.replay.core.DiffReportGenerator;
import com.amazonaws.services.replay.model.DiffReport;
import com.amazonaws.services.replay.model.ReplayJobResult;
import com.amazonaws.services.replay.model.ReplayPoint;
import com.amazonaws.services.replay.model.TimeSeriesRecord;
import com.amazonaws.services.replay.operator.DirtyRecordFilter;
import com.amazonaws.services.replay.operator.RcfDetectOperator;
import com.amazonaws.services.replay.sink.ResultCollectSink;
import com.amazonaws.services.replay.source.TimeSeriesReplaySource;
import com.amazonaws.services.replay.util.MockDataGenerator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class LocalReplayDemo {
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("  异常特征回放引擎 - 本地演示");
        System.out.println("========================================");
        System.out.println();

        long startTime = System.currentTimeMillis() - 3600000;
        long endTime = System.currentTimeMillis();
        long intervalMs = 5000;

        System.out.println("[1/5] 生成模拟数据...");
        List<ReplayPoint> points = MockDataGenerator.generateMockData(
                startTime, endTime, intervalMs, 42, 0.02, true
        );
        System.out.println("  生成数据点数量: " + points.size());
        System.out.println("  时间范围: " + new java.util.Date(startTime) +
                " ~ " + new java.util.Date(endTime));
        System.out.println("  采样间隔: " + intervalMs + "ms");
        System.out.println();

        System.out.println("[2/5] 运行基线回放任务 (默认参数)...");
        ReplayJobResult baselineResult = runReplayJob(
                "baseline-job",
                points,
                50.0,
                50,
                8192,
                1,
                3.0
        );
        System.out.println("  状态: " + baselineResult.getStatus());
        System.out.println("  总记录数: " + baselineResult.getTotalRecords());
        System.out.println("  异常检测结果数: " + baselineResult.getAnomalyResults().size());
        System.out.println("  脏数据记录数: " + baselineResult.getDirtyRecords());
        System.out.println();

        System.out.println("[3/5] 运行测试回放任务 (调整参数)...");
        ReplayJobResult testResult = runReplayJob(
                "test-job",
                points,
                50.0,
                100,
                4096,
                2,
                2.5
        );
        System.out.println("  状态: " + testResult.getStatus());
        System.out.println("  总记录数: " + testResult.getTotalRecords());
        System.out.println("  异常检测结果数: " + testResult.getAnomalyResults().size());
        System.out.println("  脏数据记录数: " + testResult.getDirtyRecords());
        System.out.println();

        System.out.println("[4/5] 生成差异报告...");
        DiffReportGenerator diffGenerator = new DiffReportGenerator("./reports");
        DiffReport report = diffGenerator.generateReport(
                "test-job",
                "baseline-job",
                testResult.getAnomalyResults(),
                baselineResult.getAnomalyResults()
        );

        String reportPath = diffGenerator.saveReport(report);
        System.out.println("  报告已保存至: " + reportPath);
        System.out.println();
        System.out.println("  ===== 差异分析摘要 =====");
        System.out.println("  对比记录数: " + report.getTotalComparedRecords());
        System.out.println("  平均得分差异: " + String.format("%.4f", report.getAvgScoreDiff()));
        System.out.println("  最大得分差异: " + String.format("%.4f", report.getMaxScoreDiff()));
        System.out.println("  异常数差异: " + report.getAnomalyCountDiff());
        System.out.println();
        System.out.println("  ===== 洞察结论 =====");
        for (String insight : report.getInsights()) {
            System.out.println("  - " + insight);
        }
        System.out.println();

        System.out.println("[5/5] 展示部分结果...");
        System.out.println();
        System.out.println("  ===== 基线异常结果 (前10个) =====");
        List<ReplayJobResult.AnomalyResult> baselineAnomalies = baselineResult.getAnomalyResults();
        int count = 0;
        for (ReplayJobResult.AnomalyResult r : baselineAnomalies) {
            if (r.isAnomaly() && count < 10) {
                System.out.println("    时间: " + new java.util.Date(r.getTimestamp()) +
                        ", 分组: " + r.getMeasureGroup() +
                        ", 得分: " + String.format("%.4f", r.getAnomalyScore()));
                count++;
            }
        }
        if (count == 0) {
            System.out.println("    (无异常点)");
        }
        System.out.println();

        System.out.println("  ===== 测试异常结果 (前10个) =====");
        List<ReplayJobResult.AnomalyResult> testAnomalies = testResult.getAnomalyResults();
        count = 0;
        for (ReplayJobResult.AnomalyResult r : testAnomalies) {
            if (r.isAnomaly() && count < 10) {
                System.out.println("    时间: " + new java.util.Date(r.getTimestamp()) +
                        ", 分组: " + r.getMeasureGroup() +
                        ", 得分: " + String.format("%.4f", r.getAnomalyScore()));
                count++;
            }
        }
        if (count == 0) {
            System.out.println("    (无异常点)");
        }
        System.out.println();

        System.out.println("  ===== 脏数据记录 (前10个) =====");
        List<ReplayJobResult.DirtyDataRecord> dirtyRecords = testResult.getDirtyDataRecords();
        for (int i = 0; i < Math.min(10, dirtyRecords.size()); i++) {
            ReplayJobResult.DirtyDataRecord d = dirtyRecords.get(i);
            System.out.println("    时间: " + new java.util.Date(d.getTimestamp()) +
                    ", 指标: " + d.getMeasureName() +
                    ", 值: " + d.getValue() +
                    ", 原因: " + d.getReason());
        }
        if (dirtyRecords.isEmpty()) {
            System.out.println("    (无脏数据)");
        }
        System.out.println();

        System.out.println("========================================");
        System.out.println("  演示完成!");
        System.out.println("========================================");

        ResultCollectSink.clearJobResults("baseline-job");
        ResultCollectSink.clearJobResults("test-job");
    }

    @SuppressWarnings("unchecked")
    private static ReplayJobResult runReplayJob(
            String jobId,
            List<ReplayPoint> points,
            double speedFactor,
            int numTrees,
            int sampleSize,
            int shingleSize,
            double zScoreThreshold) throws Exception {

        ReplayJobResult result = new ReplayJobResult();
        result.setJobId(jobId);
        result.setStatus("RUNNING");
        result.setStartTime(System.currentTimeMillis());
        result.setTotalRecords(points.size());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, Time.of(10, TimeUnit.SECONDS)
        ));
        env.setParallelism(1);

        DataStream<TimeSeriesRecord> sourceStream = env.addSource(
                new TimeSeriesReplaySource(points, speedFactor)
        ).name("TimeSeriesReplaySource");

        SingleOutputStreamOperator<TimeSeriesRecord> filteredStream = sourceStream
                .process(new DirtyRecordFilter(zScoreThreshold))
                .name("DirtyRecordFilter");

        filteredStream.getSideOutput(DirtyRecordFilter.DIRTY_DATA_TAG)
                .addSink(new ResultCollectSink<ReplayJobResult.DirtyDataRecord>(jobId, "dirty"))
                .name("DirtyDataSink");

        DataStream<ReplayJobResult.AnomalyResult> anomalyStream = filteredStream
                .keyBy(record -> record.getDimensions().getOrDefault("measure_group", "default"))
                .process(new RcfDetectOperator(
                        shingleSize,
                        false,
                        numTrees,
                        sampleSize,
                        0.00001220703125,
                        42,
                        1.0
                )).name("RcfDetectOperator");

        anomalyStream
                .addSink(new ResultCollectSink<ReplayJobResult.AnomalyResult>(jobId, "anomaly"))
                .name("AnomalyResultSink");

        env.execute("LocalReplayDemo-" + jobId);

        List<ReplayJobResult.AnomalyResult> anomalyResults =
                ResultCollectSink.getResults(jobId, "anomaly");
        List<ReplayJobResult.DirtyDataRecord> dirtyRecords =
                ResultCollectSink.getResults(jobId, "dirty");

        result.setAnomalyResults(anomalyResults);
        result.setDirtyDataRecords(dirtyRecords);
        result.setProcessedRecords(anomalyResults.size());
        result.setDirtyRecords(dirtyRecords.size());
        result.setStatus("COMPLETED");
        result.setEndTime(System.currentTimeMillis());

        return result;
    }
}
