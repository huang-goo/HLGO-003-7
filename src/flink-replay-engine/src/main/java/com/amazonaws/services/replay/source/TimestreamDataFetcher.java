package com.amazonaws.services.replay.source;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClientBuilder;
import com.amazonaws.services.timestreamquery.model.*;
import com.amazonaws.services.replay.model.ReplayPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TimestreamDataFetcher {
    private static final Logger logger = LoggerFactory.getLogger(TimestreamDataFetcher.class);

    private final AmazonTimestreamQuery queryClient;
    private final String databaseName;
    private final String tableName;
    private final String region;

    public TimestreamDataFetcher(String region, String databaseName, String tableName) {
        this.region = region;
        this.databaseName = databaseName;
        this.tableName = tableName;

        ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        this.queryClient = AmazonTimestreamQueryClientBuilder.standard()
                .withRegion(region)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    public List<ReplayPoint> fetchData(long startTime, long endTime, List<String> measureNames) {
        String query = buildQuery(startTime, endTime, measureNames);
        logger.info("Executing Timestream query: {}", query);

        List<ReplayPoint> points = new ArrayList<>();
        String nextToken = null;

        do {
            QueryRequest queryRequest = new QueryRequest()
                    .withQueryString(query)
                    .withMaxRows(1000);

            if (nextToken != null) {
                queryRequest.setNextToken(nextToken);
            }

            QueryResult result = queryClient.query(queryRequest);
            points.addAll(parseResult(result));
            nextToken = result.getNextToken();

        } while (nextToken != null);

        logger.info("Fetched {} points from Timestream", points.size());
        return points;
    }

    public List<ReplayPoint> fetchDataPaginated(long startTime, long endTime, List<String> measureNames,
                                                int maxRows, String nextToken) {
        String query = buildQuery(startTime, endTime, measureNames);

        QueryRequest queryRequest = new QueryRequest()
                .withQueryString(query)
                .withMaxRows(maxRows);

        if (nextToken != null) {
            queryRequest.setNextToken(nextToken);
        }

        QueryResult result = queryClient.query(queryRequest);
        return parseResult(result);
    }

    private String buildQuery(long startTime, long endTime, List<String> measureNames) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT measure_name, measure_value::double, time ");

        if (measureNames != null && !measureNames.isEmpty()) {
            query.append(", ");
            for (int i = 0; i < measureNames.size(); i++) {
                if (i > 0) query.append(", ");
                query.append("\"").append(measureNames.get(i)).append("\"");
            }
        }

        query.append(" FROM \"").append(databaseName).append("\".\"").append(tableName).append("\"");
        query.append(" WHERE time BETWEEN from_milliseconds(").append(startTime).append(") ");
        query.append("AND from_milliseconds(").append(endTime).append(") ");

        if (measureNames != null && !measureNames.isEmpty()) {
            query.append("AND measure_name IN (");
            for (int i = 0; i < measureNames.size(); i++) {
                if (i > 0) query.append(", ");
                query.append("'").append(measureNames.get(i)).append("'");
            }
            query.append(") ");
        }

        query.append("ORDER BY time ASC");
        return query.toString();
    }

    private List<ReplayPoint> parseResult(QueryResult result) {
        List<ReplayPoint> points = new ArrayList<>();
        List<ColumnInfo> columnInfo = result.getColumnInfo();
        List<Row> rows = result.getRows();

        if (rows == null || rows.isEmpty()) {
            return points;
        }

        int measureNameIdx = -1;
        int measureValueIdx = -1;
        int timeIdx = -1;

        for (int i = 0; i < columnInfo.size(); i++) {
            String name = columnInfo.get(i).getName();
            if ("measure_name".equals(name)) {
                measureNameIdx = i;
            } else if ("time".equals(name)) {
                timeIdx = i;
            } else if (name.startsWith("measure_value")) {
                measureValueIdx = i;
            }
        }

        for (Row row : rows) {
            List<Datum> data = row.getData();
            ReplayPoint point = new ReplayPoint();

            if (measureNameIdx >= 0 && data.get(measureNameIdx) != null) {
                point.setMeasureName(data.get(measureNameIdx).getScalarValue());
            }

            if (measureValueIdx >= 0 && data.get(measureValueIdx) != null) {
                point.setMeasureValue(data.get(measureValueIdx).getScalarValue());
                point.setMeasureValueType("DOUBLE");
            }

            if (timeIdx >= 0 && data.get(timeIdx) != null) {
                String timeStr = data.get(timeIdx).getScalarValue();
                point.setTime(parseTime(timeStr));
                point.setTimeUnit("MILLISECONDS");
            }

            Map<String, String> dimensions = new HashMap<>();
            for (int i = 0; i < columnInfo.size(); i++) {
                String colName = columnInfo.get(i).getName();
                if (!"measure_name".equals(colName) && !"time".equals(colName) &&
                        !colName.startsWith("measure_value")) {
                    Datum datum = data.get(i);
                    if (datum != null && datum.getScalarValue() != null) {
                        dimensions.put(colName, datum.getScalarValue());
                    }
                }
            }
            point.setDimensions(dimensions);

            if (point.getMeasureName() != null) {
                points.add(point);
            }
        }

        return points;
    }

    private long parseTime(String timeStr) {
        try {
            if (timeStr.endsWith("Z")) {
                return java.time.Instant.parse(timeStr).toEpochMilli();
            }
            return Long.parseLong(timeStr);
        } catch (Exception e) {
            logger.warn("Failed to parse time: {}", timeStr, e);
            return System.currentTimeMillis();
        }
    }

    public long getRecordCount(long startTime, long endTime, List<String> measureNames) {
        String query = "SELECT COUNT(*) as cnt FROM \"" + databaseName + "\".\"" + tableName + "\"" +
                " WHERE time BETWEEN from_milliseconds(" + startTime + ") " +
                "AND from_milliseconds(" + endTime + ")";

        if (measureNames != null && !measureNames.isEmpty()) {
            query += " AND measure_name IN (";
            for (int i = 0; i < measureNames.size(); i++) {
                if (i > 0) query += ", ";
                query += "'" + measureNames.get(i) + "'";
            }
            query += ")";
        }

        QueryRequest queryRequest = new QueryRequest().withQueryString(query);
        QueryResult result = queryClient.query(queryRequest);

        if (result.getRows() != null && !result.getRows().isEmpty()) {
            String countStr = result.getRows().get(0).getData().get(0).getScalarValue();
            return Long.parseLong(countStr);
        }
        return 0;
    }

    public void close() {
        if (queryClient != null) {
            queryClient.shutdown();
        }
    }
}
