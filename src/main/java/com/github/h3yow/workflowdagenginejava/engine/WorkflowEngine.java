package com.github.h3yow.workflowdagenginejava.engine;

import com.github.h3yow.workflowdagenginejava.config.EngineProperties;
import com.github.h3yow.workflowdagenginejava.handler.StepHandlerRegistry;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.StepRecord;
import com.github.h3yow.workflowdagenginejava.model.StepStatus;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import com.github.h3yow.workflowdagenginejava.store.WorkflowStore;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidator;
import com.github.h3yow.workflowdagenginejava.validation.WorkflowValidator.ValidatedWorkflow;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Core DAG scheduling engine.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>validate submissions (delegated to {@link WorkflowValidator});</li>
 *   <li>persist every state transition through {@link WorkflowStore};</li>
 *   <li>run independent steps in parallel up to the workflow's maxConcurrency;</li>
 *   <li>stop downstream steps on failure (they become SKIPPED) and never rerun
 *       already SUCCEEDED steps;</li>
 *   <li>attribute failures as DEFINITION_ERROR / STEP_EXCEPTION / TIMEOUT;</li>
 *   <li>resume PENDING/RUNNING workflows on startup, reusing persisted
 *       progress (idempotent across restart and duplicate submission).</li>
 * </ul>
 */
@Component
public class WorkflowEngine {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);
    private static final long DEFAULT_STEP_TIMEOUT_MILLIS = 10_000L;

    private final WorkflowStore store;
    private final StepHandlerRegistry registry;
    private final WorkflowValidator validator;
    private final int defaultConcurrency;

    /** Workflows currently executing in this process. */
    private final Set<String> active = ConcurrentHashMap.newKeySet();

    /** One virtual-thread executor shared for dispatch + timeout control. */
    private ExecutorService controlExecutor;
    private ScheduledExecutorService timeoutExecutor;

    public WorkflowEngine(WorkflowStore store, StepHandlerRegistry registry,
                          WorkflowValidator validator, EngineProperties properties) {
        this.store = store;
        this.registry = registry;
        this.validator = validator;
        this.defaultConcurrency = Math.max(1, properties.getDefaultConcurrency());
        this.controlExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.timeoutExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "wf-timeout");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    void recoverOnStartup() {
        recoverUnfinished();
    }

    /**
     * Test/manual entry point: resume unfinished workflows from the store,
     * mirroring the Spring startup sequence.
     */
    public void recoverOnStartupForTest() {
        recoverUnfinished();
    }

    private void recoverUnfinished() {
        List<WorkflowSnapshot> all = store.findAll();
        for (WorkflowSnapshot snap : all) {
            if (snap.status() == WorkflowStatus.PENDING || snap.status() == WorkflowStatus.RUNNING) {
                log.info("recovering unfinished workflow {} (status={}) from persisted state",
                        snap.workflowId(), snap.status());
                launch(snap);
            } else {
                log.info("workflow {} already terminal ({}), skipping recovery",
                        snap.workflowId(), snap.status());
            }
        }
    }

    @PreDestroy
    void shutdown() {
        timeoutExecutor.shutdownNow();
        controlExecutor.shutdownNow();
    }

    /** Result of a submission: the current snapshot plus whether it was newly created. */
    public record SubmissionResult(WorkflowSnapshot snapshot, boolean created) {
    }

    /**
     * Validate, persist (if the id is new) and start asynchronous execution.
     *
     * @throws DuplicateWorkflowException same id, different definition
     */
    public SubmissionResult submit(WorkflowDef def) {
        ValidatedWorkflow vw = validator.validate(def);
        int effectiveConcurrency = def.maxConcurrency() > 0
                ? def.maxConcurrency()
                : defaultConcurrency;
        WorkflowDef canonical = new WorkflowDef(vw.def().workflowId(), vw.def().steps(),
                effectiveConcurrency);

        Optional<WorkflowSnapshot> existing = store.find(canonical.workflowId());
        if (existing.isPresent()) {
            WorkflowSnapshot prev = existing.get();
            if (!sameDefinition(prev, canonical)) {
                throw new DuplicateWorkflowException(canonical.workflowId());
            }
            log.info("duplicate submission of workflow {} accepted as idempotent replay",
                    canonical.workflowId());
            launchIfNeeded(prev);
            return new SubmissionResult(prev, false);
        }

        long now = System.currentTimeMillis();
        Map<String, StepRecord> records = new LinkedHashMap<>();
        for (StepDef step : canonical.steps()) {
            records.put(step.id(), new StepRecord(StepStatus.PENDING, null, null, 0, 0L, 0L));
        }
        WorkflowSnapshot snapshot = new WorkflowSnapshot(
                canonical.workflowId(), WorkflowStatus.PENDING, canonical.maxConcurrency(),
                canonical.steps(), records, null, null, now, now);
        if (!store.insertIfAbsent(snapshot)) {
            // Lost a race with a concurrent submit of the same id: replay existing.
            WorkflowSnapshot raced = store.find(canonical.workflowId()).orElseThrow();
            launchIfNeeded(raced);
            return new SubmissionResult(raced, false);
        }
        log.info("accepted workflow {} with {} steps", canonical.workflowId(),
                canonical.steps().size());
        launch(snapshot);
        return new SubmissionResult(snapshot, true);
    }

    public Optional<WorkflowSnapshot> get(String workflowId) {
        return store.find(workflowId);
    }

    private void launchIfNeeded(WorkflowSnapshot snapshot) {
        if (snapshot.status() == WorkflowStatus.PENDING || snapshot.status() == WorkflowStatus.RUNNING) {
            launch(snapshot);
        }
    }

    private void launch(WorkflowSnapshot snapshot) {
        if (!active.add(snapshot.workflowId())) {
            return; // already running in this process
        }
        controlExecutor.submit(() -> {
            try {
                new WorkflowDispatcher(snapshot, store, registry, timeoutExecutor,
                        DEFAULT_STEP_TIMEOUT_MILLIS).run();
            } catch (Throwable t) {
                log.error("dispatcher for workflow {} died unexpectedly", snapshot.workflowId(), t);
            } finally {
                active.remove(snapshot.workflowId());
            }
        });
    }

    /** Structural equality of the definition portion of a stored snapshot. */
    private boolean sameDefinition(WorkflowSnapshot snap, WorkflowDef def) {
        if (snap.maxConcurrency() != def.maxConcurrency() || snap.steps().size() != def.steps().size()) {
            return false;
        }
        for (int i = 0; i < def.steps().size(); i++) {
            StepDef a = snap.steps().get(i);
            StepDef b = def.steps().get(i);
            if (!a.id().equals(b.id())
                    || !java.util.Objects.equals(a.action(), b.action())
                    || a.timeoutMillis() != b.timeoutMillis()
                    || !a.dependsOn().equals(b.dependsOn())) {
                return false;
            }
        }
        return true;
    }

}
