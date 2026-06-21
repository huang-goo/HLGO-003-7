package com.amazonaws.services.replay.operator;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.util.ShingleBuilder;
import com.amazonaws.services.replay.model.ReplayJobResult;
import com.amazonaws.services.replay.model.ReplayPoint;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RcfAnomalyDetector extends KeyedProcessFunction<String, ReplayPoint, ReplayJobResult.AnomalyResult>
        implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(RcfAnomalyDetector.class);

    private final int shingleSize;
    private final boolean shingleCyclic;
    private final int numberOfTrees;
    private final int sampleSize;
    private final double lambda;
    private final int randomSeed;
    private final double anomalyThreshold;

    private transient RandomCutForest forest;
    private transient ShingleBuilder shingleBuilder;
    private transient double[] pointBuffer;
    private transient double[] shingleBuffer;
    private transient Map<String, Integer> measureIndex;
    private transient int dimensionCount;

    private transient ListState<Double> forestState;
    private transient ValueState<Long> countState;

    public RcfAnomalyDetector(int shingleSize, boolean shingleCyclic, int numberOfTrees,
                              int sampleSize, double lambda, int randomSeed, double anomalyThreshold) {
        this.shingleSize = shingleSize;
        this.shingleCyclic = shingleCyclic;
        this.numberOfTrees = numberOfTrees;
        this.sampleSize = sampleSize;
        this.lambda = lambda;
        this.randomSeed = randomSeed;
        this.anomalyThreshold = anomalyThreshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        measureIndex = new HashMap<>();
        dimensionCount = 0;
    }

    @Override
    public void processElement(ReplayPoint point, Context ctx,
                               Collector<ReplayJobResult.AnomalyResult> out) throws Exception {
        String measureName = point.getMeasureName();
        double value;

        try {
            value = Double.parseDouble(point.getMeasureValue());
        } catch (NumberFormatException e) {
            return;
        }

        if (!measureIndex.containsKey(measureName)) {
            measureIndex.put(measureName, dimensionCount++);
            pointBuffer = new double[dimensionCount];
            initForest();
        }

        int idx = measureIndex.get(measureName);
        pointBuffer[idx] = value;

        if (shingleBuilder == null) {
            shingleBuilder = new ShingleBuilder(dimensionCount, shingleSize, shingleCyclic);
            shingleBuffer = new double[shingleBuilder.getShingledPointSize()];
        }

        double[] pointCopy = pointBuffer.clone();
        shingleBuilder.addPoint(pointCopy);

        if (shingleBuilder.isFull()) {
            shingleBuilder.getShingle(shingleBuffer);
            double[] shingleCopy = shingleBuffer.clone();

            double score = forest.getAnomalyScore(shingleCopy);
            forest.update(shingleCopy);

            boolean isAnomaly = score > anomalyThreshold;

            ReplayJobResult.AnomalyResult result = new ReplayJobResult.AnomalyResult(
                    point.getTime(),
                    ctx.getCurrentKey(),
                    score,
                    isAnomaly,
                    anomalyThreshold
            );
            out.collect(result);
        }
    }

    private void initForest() {
        forest = RandomCutForest.builder()
                .numberOfTrees(numberOfTrees)
                .sampleSize(sampleSize)
                .dimensions(dimensionCount * shingleSize)
                .lambda(lambda)
                .randomSeed(randomSeed)
                .build();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        forestState.clear();
        if (forest != null) {
            List<Double> state = new ArrayList<>();
            state.add((double) forest.getDimensions());
            state.add((double) forest.getNumberOfTrees());
            state.add((double) forest.getSampleSize());
            forestState.addAll(state);
        }

        if (countState != null) {
            countState.update(countState.value() != null ? countState.value() + 1 : 1L);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Double> forestDescriptor =
                new ListStateDescriptor<>("forest-state", Double.class);
        forestState = context.getOperatorStateStore().getListState(forestDescriptor);

        ValueStateDescriptor<Long> countDescriptor =
                new ValueStateDescriptor<>("count-state", Long.class);
        countState = context.getKeyedStateStore().getState(countDescriptor);

        if (context.isRestored()) {
            logger.info("Restoring RCF state from checkpoint");
        }
    }
}
