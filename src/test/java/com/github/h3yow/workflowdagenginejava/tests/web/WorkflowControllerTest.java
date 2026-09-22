package com.github.h3yow.workflowdagenginejava.tests.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.workflowdagenginejava.config.EngineProperties;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.handler.StepHandlerRegistry;
import com.github.h3yow.workflowdagenginejava.service.WorkflowService;
import com.github.h3yow.workflowdagenginejava.store.JsonFileWorkflowStore;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidator;
import com.github.h3yow.workflowdagenginejava.web.ApiExceptionHandler;
import com.github.h3yow.workflowdagenginejava.web.WorkflowController;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * End-to-end-ish HTTP tests covering the distinguishable rejection codes,
 * cycle rejection, successful completion via polling and idempotent replay.
 */
class WorkflowControllerTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowControllerTest.class);
    private static Path dataDir;
    private static MockMvc mvc;
    private static final ObjectMapper JSON = new ObjectMapper();

    @BeforeAll
    static void setup() throws Exception {
        dataDir = Files.createTempDirectory("wf-web-tests-");
        EngineProperties props = new EngineProperties();
        props.setDataDir(dataDir.toString());
        JsonFileWorkflowStore store = new JsonFileWorkflowStore(props, JSON);
        StepHandlerRegistry registry = new StepHandlerRegistry(List.of(
                new com.github.h3yow.workflowdagenginejava.handler.builtin.NoopStepHandler(),
                new com.github.h3yow.workflowdagenginejava.handler.builtin.EchoStepHandler(),
                new com.github.h3yow.workflowdagenginejava.handler.builtin.SleepStepHandler(),
                new com.github.h3yow.workflowdagenginejava.handler.builtin.FailStepHandler()));
        WorkflowEngine engine = new WorkflowEngine(store, registry, new WorkflowValidator(), props);
        WorkflowService service = new WorkflowService(engine);
        mvc = MockMvcBuilders.standaloneSetup(new WorkflowController(service))
                .setControllerAdvice(new ApiExceptionHandler())
                .build();
        log.info("web tests using data dir {}", dataDir);
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (dataDir != null) {
            try (var stream = Files.walk(dataDir)) {
                stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception ignored) {
                        // best effort
                    }
                });
            }
        }
    }

    private static String jsonWorkflow(String id, String jsonSteps, Integer concurrency) throws Exception {
        String c = concurrency == null ? "null" : concurrency.toString();
        return JSON.writeValueAsString(JSON.readTree(
                "{\"workflowId\":\"" + id + "\",\"maxConcurrency\":" + c
                        + ",\"steps\":" + jsonSteps + "}"));
    }

    @Test
    @DisplayName("POST cycle graph -> 422 CYCLIC_DEPENDENCY")
    void cycleSubmissionIsRejectedWithCode() throws Exception {
        String steps = "[{\"id\":\"a\",\"action\":\"noop\",\"dependsOn\":[\"b\"]},"
                + "{\"id\":\"b\",\"action\":\"noop\",\"dependsOn\":[\"a\"]}]";
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-cycle", steps, 2)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("CYCLIC_DEPENDENCY"));
        log.info("workflow=wf-web-cycle deps=[a->b,b->a] finalStatus=REJECTED reason=CYCLIC_DEPENDENCY");
    }

    @Test
    @DisplayName("POST missing dependency -> 422 MISSING_DEPENDENCY (distinct from cycle)")
    void missingDependencyIsRejectedWithCode() throws Exception {
        String steps = "[{\"id\":\"a\",\"action\":\"noop\",\"dependsOn\":[\"x\"]}]";
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-missing", steps, 1)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("MISSING_DEPENDENCY"));
        log.info("workflow=wf-web-missing deps=[a->x] finalStatus=REJECTED reason=MISSING_DEPENDENCY");
    }

    @Test
    @DisplayName("POST self dependency -> 422 SELF_DEPENDENCY")
    void selfDependencyIsRejectedWithCode() throws Exception {
        String steps = "[{\"id\":\"a\",\"action\":\"noop\",\"dependsOn\":[\"a\"]}]";
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-self", steps, 1)))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.error").value("SELF_DEPENDENCY"));
        log.info("workflow=wf-web-self deps=[a->a] finalStatus=REJECTED reason=SELF_DEPENDENCY");
    }

    @Test
    @DisplayName("POST malformed JSON -> 400 MALFORMED_REQUEST")
    void malformedBodyRejected() throws Exception {
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("MALFORMED_REQUEST"));
        log.info("workflow=<malformed> finalStatus=REJECTED reason=MALFORMED_REQUEST");
    }

    @Test
    @DisplayName("GET unknown id -> 404 WORKFLOW_NOT_FOUND")
    void unknownIdReturns404() throws Exception {
        mvc.perform(get("/api/workflows/does-not-exist"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("WORKFLOW_NOT_FOUND"));
    }

    @Test
    @DisplayName("submit valid workflow, poll until SUCCEEDED, resubmit returns 200 same result")
    void submitPollAndIdempotentReplay() throws Exception {
        String steps = "[{\"id\":\"a\",\"action\":\"noop\"},"
                + "{\"id\":\"b\",\"action\":\"noop\",\"dependsOn\":[\"a\"]}]";
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-ok", steps, 2)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.workflowId").value("wf-web-ok"));

        String status = "";
        for (int i = 0; i < 100; i++) {
            String body = mvc.perform(get("/api/workflows/wf-web-ok"))
                    .andExpect(status().isOk())
                    .andReturn().getResponse().getContentAsString();
            status = JSON.readTree(body).get("status").asText();
            if ("SUCCEEDED".equals(status) || "FAILED".equals(status)) {
                break;
            }
            Thread.sleep(20);
        }
        org.junit.jupiter.api.Assertions.assertEquals("SUCCEEDED", status);

        // Idempotent replay: same payload again -> 200, still SUCCEEDED.
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-ok", steps, 2)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));
        log.info("workflow=wf-web-ok deps=[b->a] finalStatus=SUCCEEDED replay=IDEMPOTENT-200");
    }

    @Test
    @DisplayName("same id with a different definition -> 409 DUPLICATE_WORKFLOW")
    void conflictingDefinitionRejected() throws Exception {
        String v1 = "[{\"id\":\"a\",\"action\":\"noop\"}]";
        String v2 = "[{\"id\":\"a\",\"action\":\"noop\"},{\"id\":\"b\",\"action\":\"noop\"}]";
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-conflict", v1, 1)))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/workflows")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(jsonWorkflow("wf-web-conflict", v2, 1)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("DUPLICATE_WORKFLOW"));
        log.info("workflow=wf-web-conflict finalStatus=REJECTED reason=DUPLICATE_WORKFLOW");
    }
}
