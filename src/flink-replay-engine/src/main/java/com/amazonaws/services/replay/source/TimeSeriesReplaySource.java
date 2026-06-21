package com.amazonaws.services.replay.source;

import com.amazonaws.services.replay.model.ReplayPoint;
import com.amazonaws.services.replay.model.TimeSeriesRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimeSeriesReplaySource implements SourceFunction<TimeSeriesRecord> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesReplaySource.class);

    private final List<ReplayPoint> points;
    private final double speedFactor;
    private final Set<String> measureFilter;
    private volatile boolean running = true;

    public TimeSeriesReplaySource(List<ReplayPoint> points, double speedFactor) {
        this(points, speedFactor, null);
    }

    public TimeSeriesReplaySource(List<ReplayPoint> points, double speedFactor, Set<String> measureFilter) {
        this.points = points;
        this.speedFactor = speedFactor;
        this.measureFilter = measureFilter;
    }

    @Override
    public void run(SourceContext<TimeSeriesRecord> ctx) throws Exception {
        if (points == null || points.isEmpty()) {
            logger.warn("No data points to replay");
            return;
        }

        logger.info("Starting replay with {} raw points, speed factor: {}", points.size(), speedFactor);

        Map<Long, TimeSeriesRecord> recordMap = new TreeMap<>();

        for (ReplayPoint point : points) {
            if (measureFilter != null && !measureFilter.contains(point.getMeasureName())) {
                continue;
            }

            long ts = point.getTime();
            TimeSeriesRecord record = recordMap.computeIfAbsent(ts, TimeSeriesRecord::new);

            try {
                double value = Double.parseDouble(point.getMeasureValue());
                record.addMeasure(point.getMeasureName(), value);
            } catch (NumberFormatException e) {
                logger.debug("Skipping non-numeric value: {} = {}", point.getMeasureName(), point.getMeasureValue());
            }

            for (Map.Entry<String, String> dim : point.getDimensions().entrySet()) {
                if (!record.getDimensions().containsKey(dim.getKey())) {
                    record.addDimension(dim.getKey(), dim.getValue());
                }
            }
        }

        List<TimeSeriesRecord> records = new ArrayList<>(recordMap.values());
        logger.info("Aggregated into {} time-series records", records.size());

        if (records.isEmpty()) {
            return;
        }

        long firstEventTime = records.get(0).getTimestamp();
        long firstSystemTime = System.currentTimeMillis();

        for (int i = 0; i < records.size() && running; i++) {
            TimeSeriesRecord record = records.get(i);

            if (speedFactor > 0 && i > 0) {
                long eventTimeDiff = record.getTimestamp() - records.get(i - 1).getTimestamp();
                long sleepTime = (long) (eventTimeDiff / speedFactor);

                if (sleepTime > 0 && sleepTime < 10000) {
                    Thread.sleep(sleepTime);
                } else if (sleepTime >= 10000) {
                    Thread.sleep(100);
                }
            }

            ctx.collectWithTimestamp(record, record.getTimestamp());

            long expectedSystemTime = firstSystemTime +
                    (long) ((record.getTimestamp() - firstEventTime) / speedFactor);
            long currentSystemTime = System.currentTimeMillis();

            if (currentSystemTime < expectedSystemTime) {
                long sleep = expectedSystemTime - currentSystemTime;
                if (sleep > 0 && sleep < 60000) {
                    Thread.sleep(sleep);
                }
            }

            if (i % 100 == 0) {
                ctx.emitWatermark(new Watermark(record.getTimestamp() - 1));
            }
        }

        if (running && !records.isEmpty()) {
            ctx.emitWatermark(new Watermark(records.get(records.size() - 1).getTimestamp() + 1));
        }

        logger.info("Replay completed. Total records: {}", records.size());
    }

    @Override
    public void cancel() {
        running = false;
        logger.info("TimeSeries replay source cancelled");
    }
}
