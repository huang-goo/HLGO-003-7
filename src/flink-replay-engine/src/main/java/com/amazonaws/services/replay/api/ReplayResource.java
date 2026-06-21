package com.amazonaws.services.replay.api;

import com.amazonaws.services.replay.core.ReplayEngine;
import com.amazonaws.services.replay.core.DiffReportGenerator;
import com.amazonaws.services.replay.model.DiffReport;
import com.amazonaws.services.replay.model.ReplayJobRequest;
import com.amazonaws.services.replay.model.ReplayJobResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/api/v1/replay")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ReplayResource {
    private final ReplayEngine engine;
    private final DiffReportGenerator diffReportGenerator;
    private final Gson gson;

    public ReplayResource() {
        this.engine = ReplayEngine.getInstance();
        this.diffReportGenerator = new DiffReportGenerator("./reports");
        this.gson = new GsonBuilder().setPrettyPrinting().create();
    }

    @POST
    @Path("/jobs")
    public Response startJob(ReplayJobRequest request) {
        try {
            ReplayJobResult result = engine.startReplayJob(request);
            return Response.status(Response.Status.ACCEPTED)
                    .entity(result)
                    .build();
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(error)
                    .build();
        }
    }

    @GET
    @Path("/jobs/{jobId}")
    public Response getJobStatus(@PathParam("jobId") String jobId) {
        ReplayJobResult result = engine.getJobResult(jobId);
        if (result == null) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Job not found: " + jobId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(error)
                    .build();
        }
        return Response.ok(result).build();
    }

    @GET
    @Path("/jobs/{jobId}/anomalies")
    public Response getAnomalyResults(
            @PathParam("jobId") String jobId,
            @QueryParam("limit") @DefaultValue("100") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        List<ReplayJobResult.AnomalyResult> results = engine.getAnomalyResults(jobId);
        if (results == null) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Job not found: " + jobId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(error)
                    .build();
        }

        int end = Math.min(offset + limit, results.size());
        List<ReplayJobResult.AnomalyResult> paginated =
                results.subList(offset, end);

        Map<String, Object> response = new HashMap<>();
        response.put("total", results.size());
        response.put("limit", limit);
        response.put("offset", offset);
        response.put("data", paginated);

        return Response.ok(response).build();
    }

    @GET
    @Path("/jobs/{jobId}/dirty")
    public Response getDirtyRecords(
            @PathParam("jobId") String jobId,
            @QueryParam("limit") @DefaultValue("100") int limit,
            @QueryParam("offset") @DefaultValue("0") int offset) {
        List<ReplayJobResult.DirtyDataRecord> records = engine.getDirtyRecords(jobId);
        if (records == null) {
            Map<String, String> error = new HashMap<>();
            error.put("error", "Job not found: " + jobId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(error)
                    .build();
        }

        int end = Math.min(offset + limit, records.size());
        List<ReplayJobResult.DirtyDataRecord> paginated =
                records.subList(offset, end);

        Map<String, Object> response = new HashMap<>();
        response.put("total", records.size());
        response.put("limit", limit);
        response.put("offset", offset);
        response.put("data", paginated);

        return Response.ok(response).build();
    }

    @POST
    @Path("/jobs/{jobId}/stop")
    public Response stopJob(@PathParam("jobId") String jobId) {
        boolean stopped = engine.stopJob(jobId);
        Map<String, Object> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("stopped", stopped);
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/jobs/{jobId}")
    public Response cleanupJob(@PathParam("jobId") String jobId) {
        boolean cleaned = engine.cleanupJob(jobId);
        Map<String, Object> response = new HashMap<>();
        response.put("jobId", jobId);
        response.put("cleaned", cleaned);
        return Response.ok(response).build();
    }

    @POST
    @Path("/diff")
    public Response generateDiffReport(DiffRequest diffRequest) {
        try {
            List<ReplayJobResult.AnomalyResult> currentResults =
                    engine.getAnomalyResults(diffRequest.getJobId());
            List<ReplayJobResult.AnomalyResult> baselineResults =
                    engine.getAnomalyResults(diffRequest.getBaselineJobId());

            if (currentResults == null || currentResults.isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("error", "Current job results not found or empty: " + diffRequest.getJobId());
                return Response.status(Response.Status.NOT_FOUND).entity(error).build();
            }

            if (baselineResults == null || baselineResults.isEmpty()) {
                Map<String, String> error = new HashMap<>();
                error.put("error", "Baseline job results not found or empty: " + diffRequest.getBaselineJobId());
                return Response.status(Response.Status.NOT_FOUND).entity(error).build();
            }

            DiffReport report = diffReportGenerator.generateReport(
                    diffRequest.getJobId(),
                    diffRequest.getBaselineJobId(),
                    currentResults,
                    baselineResults
            );

            String reportPath = diffReportGenerator.saveReport(report);
            report.getInsights().add("报告已保存至: " + reportPath);

            return Response.ok(report).build();

        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(error)
                    .build();
        }
    }

    @GET
    @Path("/health")
    public Response healthCheck() {
        Map<String, Object> health = new HashMap<>();
        health.put("status", "UP");
        health.put("service", "anomaly-replay-engine");
        health.put("version", "1.0.0");
        return Response.ok(health).build();
    }

    public static class DiffRequest {
        private String jobId;
        private String baselineJobId;

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getBaselineJobId() {
            return baselineJobId;
        }

        public void setBaselineJobId(String baselineJobId) {
            this.baselineJobId = baselineJobId;
        }
    }
}
