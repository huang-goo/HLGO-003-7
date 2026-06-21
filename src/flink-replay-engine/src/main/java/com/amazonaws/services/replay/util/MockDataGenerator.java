package com.amazonaws.services.replay.util;

import com.amazonaws.services.replay.model.ReplayPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MockDataGenerator {
    private static final String[] MEASURE_NAMES = {
            "xmeas_1", "xmeas_2", "xmeas_3", "xmeas_4", "xmeas_5",
            "xmeas_6", "xmeas_7", "xmeas_8", "xmeas_9", "xmeas_10",
            "xmeas_11", "xmeas_12", "xmeas_13", "xmeas_14", "xmeas_15",
            "xmeas_16", "xmeas_17", "xmeas_18", "xmeas_19", "xmeas_20",
            "xmeas_21", "xmeas_22"
    };

    private static final double[] BASE_VALUES = {
            0.2711, 3649.0, 4529.0, 9.261, 26.96,
            43.45, 2779.0, 73.42, 120.4, 12.93,
            53.95, 55.08, 49.82, 44.71, 12.07,
            55.81, 26.66, 42.76, 64.99, 26.12,
            4.481, 9.12
    };

    public static List<ReplayPoint> generateMockData(long startTime, long endTime, long intervalMs) {
        return generateMockData(startTime, endTime, intervalMs, 42, 0.02, false);
    }

    public static List<ReplayPoint> generateMockData(
            long startTime, long endTime, long intervalMs,
            int randomSeed, double noiseLevel, boolean injectAnomalies) {

        List<ReplayPoint> points = new ArrayList<>();
        Random random = new Random(randomSeed);

        long currentTime = startTime;
        int anomalyCounter = 0;

        while (currentTime <= endTime) {
            for (int i = 0; i < MEASURE_NAMES.length; i++) {
                double baseValue = BASE_VALUES[i % BASE_VALUES.length];
                double noise = (random.nextDouble() - 0.5) * 2 * noiseLevel * baseValue;
                double value = baseValue + noise;

                if (injectAnomalies && anomalyCounter % 100 == 0 && i == 5) {
                    value = baseValue * (1.5 + random.nextDouble() * 0.5);
                }

                ReplayPoint point = new ReplayPoint(
                        MEASURE_NAMES[i],
                        String.format("%.4f", value),
                        currentTime
                );
                point.addDimension("factory", "test-factory-1");
                point.addDimension("sensor", "sensor-" + (i + 1));
                points.add(point);
            }

            if (injectAnomalies && anomalyCounter % 150 == 0) {
                ReplayPoint dirtyPoint = new ReplayPoint(
                        "xmeas_10",
                        "INVALID",
                        currentTime
                );
                dirtyPoint.addDimension("factory", "test-factory-1");
                points.add(dirtyPoint);
            }

            anomalyCounter++;
            currentTime += intervalMs;
        }

        return points;
    }

    public static List<ReplayPoint> generateSimpleTimeSeries(
            int numPoints, long startTime, long intervalMs, String measureName) {

        List<ReplayPoint> points = new ArrayList<>();
        Random random = new Random(42);
        double baseValue = 100.0;

        for (int i = 0; i < numPoints; i++) {
            long time = startTime + (long) i * intervalMs;
            double trend = i * 0.01;
            double seasonality = Math.sin(i * 0.1) * 5;
            double noise = random.nextGaussian() * 2;
            double value = baseValue + trend + seasonality + noise;

            if (i % 50 == 0 && i > 0) {
                value += 20;
            }

            ReplayPoint point = new ReplayPoint(
                    measureName,
                    String.format("%.4f", value),
                    time
            );
            point.addDimension("group", "test-group");
            points.add(point);
        }

        return points;
    }
}
