package com.amazonaws.services.replay.operator;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.util.ShingleBuilder;
import com.amazonaws.services.replay.model.ReplayJobResult;
import com.amazonaws.services.replay.model.TimeSeriesRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RcfDetectOperator extends KeyedProcessFunction<String, TimeSeriesRecord, ReplayJobResult.AnomalyResult> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(RcfDetectOperator.class);

    private final int shingleSize;
    private final boolean shingleCyclic;
    private final int numberOfTrees;
    private final int sampleSize;
    private final double lambda;
    private final int randomSeed;
    private final double anomalyThreshold;

    private transient RandomCutForest forest;
    private transient ShingleBuilder shingleBuilder;
    private transient double[] shingleBuffer;
    private transient int dimensions;
    private transient boolean initialized;

    public RcfDetectOperator(int shingleSize, boolean shingleCyclic, int numberOfTrees,
                             int sampleSize, double lambda, int randomSeed, double anomalyThreshold) {
        this.shingleSize = shingleSize;
        this.shingleCyclic = shingleCyclic;
        this.numberOfTrees = numberOfTrees;
        this.sampleSize = sampleSize;
        this.lambda = lambda;
        this.randomSeed = randomSeed;
        this.anomalyThreshold = anomalyThreshold;
        this.initialized = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.initialized = false;
    }

    private void init(int dimCount) {
        this.dimensions = dimCount;

        shingleBuilder = new ShingleBuilder(dimensions, shingleSize, shingleCyclic);
        shingleBuffer = new double[shingleBuilder.getShingledPointSize()];

        forest = RandomCutForest.builder()
                .numberOfTrees(numberOfTrees)
                .sampleSize(sampleSize)
                .dimensions(shingleBuilder.getShingledPointSize())
                .lambda(lambda)
                .randomSeed(randomSeed)
                .build();

        initialized = true;
        logger.info("RCF initialized: dimensions={}, shingleSize={}, trees={}, sampleSize={}",
                dimensions, shingleSize, numberOfTrees, sampleSize);
    }

    @Override
    public void processElement(TimeSeriesRecord record, Context ctx,
                               Collector<ReplayJobResult.AnomalyResult> out) throws Exception {
        if (!initialized) {
            init(record.getDimensionCount());
        }

        double[] values = record.getValuesAsArray();

        if (values.length != dimensions) {
            logger.debug("Skipping record with unexpected dimension count: expected {}, got {}",
                    dimensions, values.length);
            return;
        }

        shingleBuilder.addPoint(values);

        if (shingleBuilder.isFull()) {
            shingleBuilder.getShingle(shingleBuffer);

            double score = forest.getAnomalyScore(shingleBuffer);
            forest.update(shingleBuffer);

            boolean isAnomaly = score > anomalyThreshold;

            ReplayJobResult.AnomalyResult result = new ReplayJobResult.AnomalyResult(
                    record.getTimestamp(),
                    ctx.getCurrentKey(),
                    score,
                    isAnomaly,
                    anomalyThreshold
            );
            out.collect(result);
        }
    }
}
