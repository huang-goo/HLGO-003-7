package com.amazonaws.services.replay.operator;

import com.amazonaws.services.replay.model.ReplayPoint;
import com.amazonaws.services.replay.model.ReplayJobResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DirtyDataFilter extends ProcessFunction<ReplayPoint, ReplayPoint> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(DirtyDataFilter.class);

    public static final OutputTag<ReplayJobResult.DirtyDataRecord> DIRTY_DATA_TAG =
            new OutputTag<ReplayJobResult.DirtyDataRecord>("dirty-data") {};

    private final double zScoreThreshold;
    private Map<String, Double> meanMap;
    private Map<String, Double> stdMap;
    private Map<String, Integer> countMap;
    private Map<String, Double> sumMap;
    private Map<String, Double> sumSqMap;

    public DirtyDataFilter(double zScoreThreshold) {
        this.zScoreThreshold = zScoreThreshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        meanMap = new HashMap<>();
        stdMap = new HashMap<>();
        countMap = new HashMap<>();
        sumMap = new HashMap<>();
        sumSqMap = new HashMap<>();
    }

    @Override
    public void processElement(ReplayPoint point, Context ctx, Collector<ReplayPoint> out) throws Exception {
        String measureName = point.getMeasureName();
        double value;

        try {
            value = Double.parseDouble(point.getMeasureValue());
        } catch (NumberFormatException e) {
            emitDirtyData(ctx, point, "Invalid number format: " + point.getMeasureValue());
            return;
        }

        if (Double.isNaN(value) || Double.isInfinite(value)) {
            emitDirtyData(ctx, point, "NaN or Infinite value");
            return;
        }

        int count = countMap.getOrDefault(measureName, 0);
        double sum = sumMap.getOrDefault(measureName, 0.0);
        double sumSq = sumSqMap.getOrDefault(measureName, 0.0);

        count++;
        sum += value;
        sumSq += value * value;

        countMap.put(measureName, count);
        sumMap.put(measureName, sum);
        sumSqMap.put(measureName, sumSq);

        if (count > 10) {
            double mean = sum / count;
            double variance = (sumSq / count) - (mean * mean);
            double std = Math.sqrt(Math.max(variance, 0));

            meanMap.put(measureName, mean);
            stdMap.put(measureName, std);

            if (std > 0) {
                double zScore = Math.abs((value - mean) / std);
                if (zScore > zScoreThreshold) {
                    emitDirtyData(ctx, point,
                            String.format("Z-score %.2f exceeds threshold %.2f (mean=%.2f, std=%.2f)",
                                    zScore, zScoreThreshold, mean, std));
                    return;
                }
            }
        }

        out.collect(point);
    }

    private void emitDirtyData(Context ctx, ReplayPoint point, String reason) {
        ReplayJobResult.DirtyDataRecord dirtyRecord = new ReplayJobResult.DirtyDataRecord(
                point.getTime(),
                point.getMeasureName(),
                point.getMeasureValue(),
                reason
        );
        ctx.output(DIRTY_DATA_TAG, dirtyRecord);
        logger.debug("Dirty data detected: measure={}, value={}, reason={}",
                point.getMeasureName(), point.getMeasureValue(), reason);
    }
}
