package com.amazonaws.services.replay.operator;

import com.amazonaws.services.replay.model.ReplayJobResult;
import com.amazonaws.services.replay.model.TimeSeriesRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DirtyRecordFilter extends ProcessFunction<TimeSeriesRecord, TimeSeriesRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(DirtyRecordFilter.class);

    public static final OutputTag<ReplayJobResult.DirtyDataRecord> DIRTY_DATA_TAG =
            new OutputTag<ReplayJobResult.DirtyDataRecord>("dirty-data") {};

    private final double zScoreThreshold;

    private Map<String, Double> meanMap;
    private Map<String, Double> stdMap;
    private Map<String, Long> countMap;
    private Map<String, Double> sumMap;
    private Map<String, Double> sumSqMap;

    public DirtyRecordFilter(double zScoreThreshold) {
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
    public void processElement(TimeSeriesRecord record, Context ctx,
                               Collector<TimeSeriesRecord> out) throws Exception {
        boolean recordIsDirty = false;

        for (Map.Entry<String, Double> entry : record.getMeasures().entrySet()) {
            String measureName = entry.getKey();
            double value = entry.getValue();

            if (Double.isNaN(value) || Double.isInfinite(value)) {
                emitDirtyData(ctx, record.getTimestamp(), measureName,
                        String.valueOf(value), "NaN or Infinite value");
                recordIsDirty = true;
                continue;
            }

            long count = countMap.getOrDefault(measureName, 0L);
            double sum = sumMap.getOrDefault(measureName, 0.0);
            double sumSq = sumSqMap.getOrDefault(measureName, 0.0);

            count++;
            sum += value;
            sumSq += value * value;

            countMap.put(measureName, count);
            sumMap.put(measureName, sum);
            sumSqMap.put(measureName, sumSq);

            if (count > 20) {
                double mean = sum / count;
                double variance = (sumSq / count) - (mean * mean);
                double std = Math.sqrt(Math.max(variance, 0));

                meanMap.put(measureName, mean);
                stdMap.put(measureName, std);

                if (std > 0) {
                    double zScore = Math.abs((value - mean) / std);
                    if (zScore > zScoreThreshold) {
                        emitDirtyData(ctx, record.getTimestamp(), measureName,
                                String.valueOf(value),
                                String.format("Z-score %.2f exceeds threshold %.2f",
                                        zScore, zScoreThreshold));
                        recordIsDirty = true;
                    }
                }
            }
        }

        if (!recordIsDirty) {
            out.collect(record);
        }
    }

    private void emitDirtyData(Context ctx, long timestamp, String measureName,
                               String value, String reason) {
        ReplayJobResult.DirtyDataRecord dirtyRecord = new ReplayJobResult.DirtyDataRecord(
                timestamp, measureName, value, reason
        );
        ctx.output(DIRTY_DATA_TAG, dirtyRecord);
        logger.debug("Dirty data detected: measure={}, value={}, reason={}", measureName, value, reason);
    }
}
