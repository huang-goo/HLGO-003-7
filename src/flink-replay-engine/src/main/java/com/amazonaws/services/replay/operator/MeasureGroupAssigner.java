package com.amazonaws.services.replay.operator;

import com.amazonaws.services.replay.model.ReplayPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MeasureGroupAssigner extends RichMapFunction<ReplayPoint, ReplayPoint> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(MeasureGroupAssigner.class);

    private final Map<String, String> measureToGroupMap;
    private final String defaultGroup;

    public MeasureGroupAssigner() {
        this.measureToGroupMap = new HashMap<>();
        this.defaultGroup = "default";
        initDefaultGroups();
    }

    private void initDefaultGroups() {
        for (int i = 1; i <= 22; i++) {
            measureToGroupMap.put("xmeas_" + i, "stream6");
        }
        for (int i = 6; i <= 9; i++) {
            measureToGroupMap.put("xmeas_" + i, "stream6");
        }
        for (int i = 10; i <= 13; i++) {
            measureToGroupMap.put("xmeas_" + i, "stream9");
        }
        for (int i = 15; i <= 19; i++) {
            measureToGroupMap.put("xmeas_" + i, "stream11");
        }
    }

    @Override
    public ReplayPoint map(ReplayPoint point) throws Exception {
        String group = measureToGroupMap.getOrDefault(point.getMeasureName(), defaultGroup);
        point.addDimension("measure_group", group);
        return point;
    }

    public void addMeasureGroup(String measureName, String groupName) {
        measureToGroupMap.put(measureName, groupName);
    }
}
