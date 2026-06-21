package com.amazonaws.services.replay.source;

import com.amazonaws.services.replay.model.ReplayPoint;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ReplaySourceFunction implements SourceFunction<ReplayPoint> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ReplaySourceFunction.class);

    private final List<ReplayPoint> points;
    private final double speedFactor;
    private volatile boolean running = true;

    public ReplaySourceFunction(List<ReplayPoint> points, double speedFactor) {
        this.points = points;
        this.speedFactor = speedFactor;
    }

    @Override
    public void run(SourceContext<ReplayPoint> ctx) throws Exception {
        if (points == null || points.isEmpty()) {
            logger.warn("No data points to replay");
            return;
        }

        logger.info("Starting replay with {} points, speed factor: {}", points.size(), speedFactor);

        long firstEventTime = points.get(0).getTime();
        long firstSystemTime = System.currentTimeMillis();

        for (int i = 0; i < points.size() && running; i++) {
            ReplayPoint point = points.get(i);

            if (speedFactor > 0 && i > 0) {
                long eventTimeDiff = point.getTime() - points.get(i - 1).getTime();
                long sleepTime = (long) (eventTimeDiff / speedFactor);

                if (sleepTime > 0) {
                    Thread.sleep(Math.min(sleepTime, 10000));
                }
            }

            ctx.collectWithTimestamp(point, point.getTime());

            long currentEventTime = point.getTime();
            long expectedSystemTime = firstSystemTime + (long) ((currentEventTime - firstEventTime) / speedFactor);
            long currentSystemTime = System.currentTimeMillis();

            if (currentSystemTime < expectedSystemTime) {
                Thread.sleep(expectedSystemTime - currentSystemTime);
            }

            if (i % 100 == 0) {
                ctx.emitWatermark(new Watermark(point.getTime() - 1));
            }
        }

        if (running && !points.isEmpty()) {
            ctx.emitWatermark(new Watermark(points.get(points.size() - 1).getTime() + 1));
        }

        logger.info("Replay completed. Total points: {}", points.size());
    }

    @Override
    public void cancel() {
        running = false;
        logger.info("Replay source cancelled");
    }
}
