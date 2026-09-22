package com.github.h3yow.workflowdagenginejava.tests.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.h3yow.workflowdagenginejava.config.EngineProperties;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.handler.StepHandler;
import com.github.h3yow.workflowdagenginejava.handler.StepHandlerRegistry;
import com.github.h3yow.workflowdagenginejava.model.FailureKind;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.StepRecord;
import com.github.h3yow.workflowdagenginejava.model.StepStatus;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import com.github.h3yow.workflowdagenginejava.store.JsonFileWorkflowStore;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.h3yow.workflowdagenginejava.tests.engine.TestInfrastructure.awaitUntil;
import static com.github.h3yow.workflowdagenginejava.tests.engine.TestInfrastructure.deleteRecursively;
import static com.github.h3yow.workflowdagenginejava.tests.engine.TestInfrastructure.newDataDir;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Engine-level scenarios: dependency-ordered success, failure propagation
 * with downstream skipping, timeout attribution, bounded concurrency,
 * restart recovery from persisted state, and idempotent duplicate submission.
 */
class WorkflowEngineTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngineTest.class);

    private Path dataDir;

    @AfterEach
    void cleanup() {
        deleteRecursively(dataDir);
    }

    private static StepDef step(String id, String action, long timeout, String... deps) {
        return new StepDef(id, action, List.of(deps), timeout);
    }

    private EngineHandle newEngine(List<StepHandler> extraHandlers) {
        dataDir = newDataDir();
        EngineProperties props = new EngineProperties();
        props.setDataDir(dataDir.toString());
        props.setDefaultConcurrency(4);
        List<StepHandler> all = new ArrayList<>(extraHandlers);
        JsonFileWorkflowStore store = new JsonFileWorkflowStore(props, new ObjectMapper());
        StepHandlerRegistry registry = new StepHandlerRegistry(all);
        WorkflowEngine engine = new WorkflowEngine(store, registry, new WorkflowValidator(), props);
        return new EngineHandle(engine, store);
    }

    private record EngineHandle(WorkflowEngine engine, JsonFileWorkflowStore store) {
    }

    private static void logFinal(String workflowId, WorkflowSnapshot snap) {
        StringBuilder deps = new StringBuilder();
        snap.steps().forEach(s -> deps.append(s.id()).append("->").append(s.dependsOn()).append(' '));
        StringBuilder states = new StringBuilder();
        snap.records().forEach((id, r) -> states.append(id).append('=').append(r.status()).append(' '));
        log.info("workflow={} deps=[{}] finalStatus={} failureKind={} steps=[{}]",
                workflowId, deps.toString().trim(), snap.status(), snap.failureKind(),
                states.toString().trim());
    }

    private WorkflowSnapshot awaitTerminal(WorkflowEngine engine, String id) {
        awaitUntil(Duration.ofSeconds(20), () ->
                engine.get(id).map(s -> s.status() == WorkflowStatus.SUCCEEDED
                        || s.status() == WorkflowStatus.FAILED).orElse(false));
        return engine.get(id).orElseThrow();
    }

    @Test
    @DisplayName("successful DAG: steps run only after dependencies, all succeed")
    void happyPathRunsInDependencyOrder() {
        List<String> finishOrder = new CopyOnWriteArrayList<>();
        List<StepHandler> handlers = List.of(
                new TestInfrastructure.CountingHandler("a", 0, finishOrder),
                new TestInfrastructure.CountingHandler("b", 0, finishOrder),
                new TestInfrastructure.CountingHandler("c", 0, finishOrder));
        var h = newEngine(handlers);

        WorkflowDef def = new WorkflowDef("wf-happy", List.of(
                step("c", "c", 0, "a", "b"),
                step("a", "a", 0),
                step("b", "b", 0, "a")), 4);
        h.engine().submit(def);
        WorkflowSnapshot snap = awaitTerminal(h.engine(), "wf-happy");

        assertEquals(WorkflowStatus.SUCCEEDED, snap.status());
        // Submitted in order c,a,b but execution must respect deps.
        assertTrue(finishOrder.indexOf("a@1") < finishOrder.indexOf("b@1"));
        assertTrue(finishOrder.indexOf("b@1") < finishOrder.indexOf("c@1"));
        snap.records().values().forEach(r -> assertEquals(StepStatus.SUCCEEDED, r.status()));
        logFinal("wf-happy", snap);
    }

    @Test
    @DisplayName("step failure: failing step is FAILED, all downstream SKIPPED, status FAILED/STEP_EXCEPTION")
    void failurePropagatesDownstream() {
        List<String> trace = new CopyOnWriteArrayList<>();
        List<StepHandler> handlers = List.of(
                new TestInfrastructure.CountingHandler("a", 0, trace),
                new TestInfrastructure.AlwaysFailHandler("boom"),
                new TestInfrastructure.CountingHandler("c", 0, trace));
        var h = newEngine(handlers);

        // a -> b(fail) -> c ; a succeeds, b fails, c must be skipped and never run.
        WorkflowDef def = new WorkflowDef("wf-fail", List.of(
                step("a", "a", 0),
                step("b", "boom", 0, "a"),
                step("c", "c", 0, "b")), 2);
        h.engine().submit(def);
        WorkflowSnapshot snap = awaitTerminal(h.engine(), "wf-fail");

        assertEquals(WorkflowStatus.FAILED, snap.status());
        assertEquals(FailureKind.STEP_EXCEPTION, snap.failureKind());
        assertEquals(StepStatus.SUCCEEDED, snap.records().get("a").status());
        assertEquals(StepStatus.FAILED, snap.records().get("b").status());
        assertEquals(StepStatus.SKIPPED, snap.records().get("c").status());
        assertEquals(0, trace.stream().filter(t -> t.startsWith("c")).count(),
                "downstream step c must never execute");
        logFinal("wf-fail", snap);
    }

    @Test
    @DisplayName("timeout: slow step is marked TIMEOUT, downstream SKIPPED, failureKind TIMEOUT")
    void timeoutIsDistinguishable() {
        List<String> trace = new CopyOnWriteArrayList<>();
        var h = newEngine(List.of(
                new TestInfrastructure.CountingHandler("slow", 2_000, trace),
                new TestInfrastructure.CountingHandler("down", 0, trace)));

        WorkflowDef def = new WorkflowDef("wf-timeout", List.of(
                step("s", "slow", 100),
                step("d", "down", 0, "s")), 1);
        long start = System.currentTimeMillis();
        h.engine().submit(def);
        WorkflowSnapshot snap = awaitTerminal(h.engine(), "wf-timeout");

        assertEquals(WorkflowStatus.FAILED, snap.status());
        assertEquals(FailureKind.TIMEOUT, snap.failureKind());
        assertEquals(StepStatus.TIMEOUT, snap.records().get("s").status());
        assertEquals(StepStatus.SKIPPED, snap.records().get("d").status());
        assertTrue(System.currentTimeMillis() - start < 1_500, "workflow must not wait for full step duration");
        logFinal("wf-timeout", snap);
    }

    @Test
    @DisplayName("concurrency cap: never more in flight than maxConcurrency")
    void concurrencyIsBounded() {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        List<StepHandler> handlers = new ArrayList<>();
        for (String id : List.of("h0", "h1", "h2", "h3", "h4", "h5")) {
            handlers.add(new StepHandler() {
                @Override
                public String action() {
                    return id;
                }

                @Override
                public String execute(String stepId) throws Exception {
                    int now = inFlight.incrementAndGet();
                    maxObserved.updateAndGet(prev -> Math.max(prev, now));
                    Thread.sleep(150);
                    inFlight.decrementAndGet();
                    return "ok";
                }
            });
        }
        var h = newEngine(handlers);

        List<StepDef> steps = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            steps.add(step("t" + i, "h" + i, 0));
        }
        h.engine().submit(new WorkflowDef("wf-concurrency", steps, 2));
        WorkflowSnapshot snap = awaitTerminal(h.engine(), "wf-concurrency");

        assertEquals(WorkflowStatus.SUCCEEDED, snap.status());
        assertTrue(maxObserved.get() <= 2, "observed peak concurrency " + maxObserved.get());
        assertEquals(2, maxObserved.get(), "the cap should actually be reached");
        logFinal("wf-concurrency", snap);
    }

    @Test
    @DisplayName("restart recovery: steps succeeded before crash are never rerun; remaining steps resume")
    void resumesFromPersistedStateAfterRestart() {
        // Simulate a crash after step a succeeded: craft the on-disk snapshot directly.
        dataDir = newDataDir();
        String wfId = "wf-restart";
        StepDef a = step("a", "recA", 0);
        StepDef b = step("b", "recB", 0, "a");
        long now = System.currentTimeMillis();
        Map<String, StepRecord> records = Map.of(
                "a", new StepRecord(StepStatus.SUCCEEDED, "before-crash", null, 1, now - 10, now - 5),
                "b", new StepRecord(StepStatus.PENDING, null, null, 0, 0L, 0L));
        WorkflowSnapshot crashed = new WorkflowSnapshot(wfId, WorkflowStatus.RUNNING, 2,
                List.of(a, b), records, null, null, now - 20, now - 5);

        EngineProperties props = new EngineProperties();
        props.setDataDir(dataDir.toString());
        JsonFileWorkflowStore crashedStore = new JsonFileWorkflowStore(props, new ObjectMapper());
        crashedStore.save(crashed);

        // A brand-new engine over the same directory == process restart.
        List<String> traceA = new CopyOnWriteArrayList<>();
        List<String> traceB = new CopyOnWriteArrayList<>();
        JsonFileWorkflowStore restartStore = new JsonFileWorkflowStore(props, new ObjectMapper());
        StepHandlerRegistry registry = new StepHandlerRegistry(List.of(
                new TestInfrastructure.CountingHandler("recA", 0, traceA),
                new TestInfrastructure.CountingHandler("recB", 0, traceB)));
        WorkflowEngine engine2 = new WorkflowEngine(restartStore, registry,
                new WorkflowValidator(), props);

        // Trigger startup recovery the same way Spring would.
        engine2.recoverOnStartupForTest();
        WorkflowSnapshot snap = awaitTerminal(engine2, wfId);

        assertEquals(WorkflowStatus.SUCCEEDED, snap.status());
        assertEquals(0, traceA.size(), "already-succeeded step a must never rerun");
        assertEquals(1, traceB.size(), "pending step b runs exactly once after restart");
        assertEquals(1, snap.records().get("a").attempts());
        assertEquals(1, snap.records().get("b").attempts());
        logFinal(wfId, snap);
    }

    @Test
    @DisplayName("restart recovery: a step left RUNNING by a crash is retried; succeeded ones stay")
    void interruptedRunningStepIsRetriedAfterRestart() {
        dataDir = newDataDir();
        String wfId = "wf-restart-running";
        StepDef a = step("a", "recA2", 0);
        StepDef b = step("b", "recB2", 0, "a");
        long now = System.currentTimeMillis();
        Map<String, StepRecord> records = new java.util.HashMap<>();
        records.put("a", new StepRecord(StepStatus.SUCCEEDED, "done", null, 1, now - 10, now - 5));
        records.put("b", new StepRecord(StepStatus.RUNNING, "interrupted", null, 1, now - 2, 0L));
        WorkflowSnapshot crashed = new WorkflowSnapshot(wfId, WorkflowStatus.RUNNING, 2,
                List.of(a, b), records, null, null, now - 20, now - 2);

        EngineProperties props = new EngineProperties();
        props.setDataDir(dataDir.toString());
        new JsonFileWorkflowStore(props, new ObjectMapper()).save(crashed);

        List<String> traceB = new CopyOnWriteArrayList<>();
        JsonFileWorkflowStore restartStore = new JsonFileWorkflowStore(props, new ObjectMapper());
        StepHandlerRegistry registry = new StepHandlerRegistry(List.of(
                new TestInfrastructure.CountingHandler("recA2", 0, new CopyOnWriteArrayList<>()),
                new TestInfrastructure.CountingHandler("recB2", 0, traceB)));
        WorkflowEngine engine2 = new WorkflowEngine(restartStore, registry,
                new WorkflowValidator(), props);
        engine2.recoverOnStartupForTest();

        WorkflowSnapshot snap = awaitTerminal(engine2, wfId);
        assertEquals(WorkflowStatus.SUCCEEDED, snap.status());
        assertEquals(1, traceB.size());
        assertEquals(2, snap.records().get("b").attempts(), "attempts must reflect pre-crash try + retry");
        assertEquals(StepStatus.SUCCEEDED, snap.records().get("b").status());
        logFinal(wfId, snap);
    }

    @Test
    @DisplayName("idempotent resubmit: same id+definition returns same result without rerunning steps")
    void duplicateSubmissionReturnsExistingResult() {
        List<String> trace = new CopyOnWriteArrayList<>();
        var h = newEngine(List.of(new TestInfrastructure.CountingHandler("a", 0, trace)));
        WorkflowDef def = new WorkflowDef("wf-dup", List.of(step("a", "a", 0)), 1);

        var first = h.engine().submit(def);
        assertTrue(first.created());
        WorkflowSnapshot done = awaitTerminal(h.engine(), "wf-dup");
        assertEquals(1, trace.size());

        var second = h.engine().submit(def);
        assertFalse(second.created());
        assertEquals(done.records().get("a").status(),
                h.engine().get("wf-dup").orElseThrow().records().get("a").status());
        assertEquals(1, trace.size(), "resubmission must not execute any step again");
        assertEquals(done.createdAt(), second.snapshot().createdAt());
        logFinal("wf-dup", h.engine().get("wf-dup").orElseThrow());
    }
}
