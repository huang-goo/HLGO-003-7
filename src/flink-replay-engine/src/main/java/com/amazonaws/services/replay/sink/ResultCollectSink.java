package com.amazonaws.services.replay.sink;

import com.amazonaws.services.replay.model.ReplayJobResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ResultCollectSink<T> extends RichSinkFunction<T> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(ResultCollectSink.class);

    private static final ConcurrentHashMap<String, List<Object>> resultMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> countMap = new ConcurrentHashMap<>();

    private final String jobId;
    private final String resultType;

    public ResultCollectSink(String jobId, String resultType) {
        this.jobId = jobId;
        this.resultType = resultType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String key = jobId + "_" + resultType;
        resultMap.putIfAbsent(key, Collections.synchronizedList(new ArrayList<>()));
        countMap.putIfAbsent(key, 0L);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        String key = jobId + "_" + resultType;
        List<Object> results = resultMap.get(key);
        if (results != null) {
            results.add(value);
        }
        countMap.merge(key, 1L, Long::sum);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> getResults(String jobId, String resultType) {
        String key = jobId + "_" + resultType;
        List<Object> results = resultMap.get(key);
        if (results == null) {
            return new ArrayList<>();
        }
        List<T> casted = new ArrayList<>();
        for (Object obj : results) {
            casted.add((T) obj);
        }
        return casted;
    }

    public static long getCount(String jobId, String resultType) {
        String key = jobId + "_" + resultType;
        return countMap.getOrDefault(key, 0L);
    }

    public static void clearJobResults(String jobId) {
        resultMap.keySet().removeIf(key -> key.startsWith(jobId + "_"));
        countMap.keySet().removeIf(key -> key.startsWith(jobId + "_"));
    }
}
