package com.amazonaws.services.replay.core;

import com.amazonaws.services.replay.model.DiffReport;
import com.amazonaws.services.replay.model.ReplayJobResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class DiffReportGenerator {
    private static final Logger logger = LoggerFactory.getLogger(DiffReportGenerator.class);

    private final String reportDir;
    private final Gson gson;

    public DiffReportGenerator(String reportDir) {
        this.reportDir = reportDir;
        this.gson = new GsonBuilder().setPrettyPrinting().create();
        try {
            Files.createDirectories(Paths.get(reportDir));
        } catch (IOException e) {
            logger.warn("Failed to create report directory", e);
        }
    }

    public DiffReport generateReport(
            String jobId,
            String baselineJobId,
            List<ReplayJobResult.AnomalyResult> currentResults,
            List<ReplayJobResult.AnomalyResult> baselineResults) {

        DiffReport report = new DiffReport();
        report.setJobId(jobId);
        report.setBaselineJobId(baselineJobId);
        report.setGeneratedTime(System.currentTimeMillis());

        Map<String, ReplayJobResult.AnomalyResult> baselineMap = new HashMap<>();
        for (ReplayJobResult.AnomalyResult r : baselineResults) {
            String key = r.getTimestamp() + "_" + r.getMeasureGroup();
            baselineMap.put(key, r);
        }

        double totalDiff = 0;
        double maxDiff = Double.MIN_VALUE;
        double minDiff = Double.MAX_VALUE;
        int compareCount = 0;

        int currentAnomalies = 0;
        int baselineAnomalies = 0;
        int truePositives = 0;
        int falsePositives = 0;
        int falseNegatives = 0;

        List<DiffReport.DiffRecord> diffRecords = new ArrayList<>();

        for (ReplayJobResult.AnomalyResult current : currentResults) {
            String key = current.getTimestamp() + "_" + current.getMeasureGroup();
            ReplayJobResult.AnomalyResult baseline = baselineMap.get(key);

            if (baseline != null) {
                double scoreDiff = current.getAnomalyScore() - baseline.getAnomalyScore();
                totalDiff += Math.abs(scoreDiff);
                maxDiff = Math.max(maxDiff, scoreDiff);
                minDiff = Math.min(minDiff, scoreDiff);
                compareCount++;

                String diffType = "SCORE_DIFF";
                if (current.isAnomaly() && baseline.isAnomaly()) {
                    diffType = "BOTH_ANOMALY";
                    truePositives++;
                } else if (current.isAnomaly() && !baseline.isAnomaly()) {
                    diffType = "NEW_ANOMALY";
                    falsePositives++;
                } else if (!current.isAnomaly() && baseline.isAnomaly()) {
                    diffType = "MISSED_ANOMALY";
                    falseNegatives++;
                }

                if (Math.abs(scoreDiff) > 0.1 ||
                        current.isAnomaly() != baseline.isAnomaly()) {
                    DiffReport.DiffRecord record = new DiffReport.DiffRecord();
                    record.setTimestamp(current.getTimestamp());
                    record.setMeasureGroup(current.getMeasureGroup());
                    record.setBaselineScore(baseline.getAnomalyScore());
                    record.setCurrentScore(current.getAnomalyScore());
                    record.setScoreDiff(scoreDiff);
                    record.setBaselineIsAnomaly(baseline.isAnomaly());
                    record.setCurrentIsAnomaly(current.isAnomaly());
                    record.setDiffType(diffType);
                    diffRecords.add(record);
                }

                if (current.isAnomaly()) currentAnomalies++;
                if (baseline.isAnomaly()) baselineAnomalies++;
            }
        }

        report.setTotalComparedRecords(compareCount);
        report.setAvgScoreDiff(compareCount > 0 ? totalDiff / compareCount : 0);
        report.setMaxScoreDiff(maxDiff == Double.MIN_VALUE ? 0 : maxDiff);
        report.setMinScoreDiff(minDiff == Double.MAX_VALUE ? 0 : minDiff);
        report.setAnomalyCountDiff(currentAnomalies - baselineAnomalies);

        double precision = (truePositives + falsePositives) > 0 ?
                (double) truePositives / (truePositives + falsePositives) : 0;
        double recall = (truePositives + falseNegatives) > 0 ?
                (double) truePositives / (truePositives + falseNegatives) : 0;

        report.setPrecisionDiff(precision);
        report.setRecallDiff(recall);
        report.setDiffRecords(diffRecords);

        List<String> insights = generateInsights(report, currentResults.size(), baselineResults.size());
        report.setInsights(insights);

        return report;
    }

    private List<String> generateInsights(DiffReport report, int currentSize, int baselineSize) {
        List<String> insights = new ArrayList<>();

        if (report.getAvgScoreDiff() < 0.05) {
            insights.add("两次运行的异常得分高度一致，模型参数变化对整体结果影响较小。");
        } else if (report.getAvgScoreDiff() < 0.2) {
            insights.add("两次运行的异常得分存在中度差异，建议重点关注高分差异点。");
        } else {
            insights.add("两次运行的异常得分差异较大，模型参数变化对结果有显著影响。");
        }

        if (report.getAnomalyCountDiff() == 0) {
            insights.add("检测到的异常点数量相同，模型的异常判定结果一致。");
        } else if (report.getAnomalyCountDiff() > 0) {
            insights.add("当前配置比基线多检测到 " + report.getAnomalyCountDiff() +
                    " 个异常点，模型敏感度有所提升。");
        } else {
            insights.add("当前配置比基线少检测到 " + Math.abs(report.getAnomalyCountDiff()) +
                    " 个异常点，模型敏感度有所下降。");
        }

        int newAnomalies = 0;
        int missedAnomalies = 0;
        for (DiffReport.DiffRecord record : report.getDiffRecords()) {
            if ("NEW_ANOMALY".equals(record.getDiffType())) {
                newAnomalies++;
            } else if ("MISSED_ANOMALY".equals(record.getDiffType())) {
                missedAnomalies++;
            }
        }

        if (newAnomalies > 0) {
            insights.add("新增异常点 " + newAnomalies + " 个，可能是由于参数调整提高了模型敏感度。");
        }
        if (missedAnomalies > 0) {
            insights.add("漏检异常点 " + missedAnomalies + " 个，可能是由于参数调整降低了模型敏感度。");
        }

        if (report.getDiffRecords().size() > 0) {
            insights.add("共有 " + report.getDiffRecords().size() +
                    " 个显著差异点，建议结合业务场景进行人工复核。");
        }

        return insights;
    }

    public String saveReport(DiffReport report) throws IOException {
        String fileName = "diff_report_" + report.getJobId() + "_" +
                System.currentTimeMillis() + ".json";
        Path filePath = Paths.get(reportDir, fileName);

        try (FileWriter writer = new FileWriter(filePath.toFile())) {
            gson.toJson(report, writer);
        }

        logger.info("Diff report saved to: {}", filePath);
        return filePath.toString();
    }

    public DiffReport loadReport(String reportPath) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(reportPath)));
        return gson.fromJson(content, DiffReport.class);
    }

    public String getReportDir() {
        return reportDir;
    }
}
