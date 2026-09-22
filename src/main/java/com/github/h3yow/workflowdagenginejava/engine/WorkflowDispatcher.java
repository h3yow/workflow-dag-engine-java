package com.github.h3yow.workflowdagenginejava.engine;

import com.github.h3yow.workflowdagenginejava.handler.StepHandler;
import com.github.h3yow.workflowdagenginejava.handler.StepHandlerRegistry;
import com.github.h3yow.workflowdagenginejava.model.FailureKind;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.StepRecord;
import com.github.h3yow.workflowdagenginejava.model.StepStatus;
import com.github.h3yow.workflowdagenginejava.model.WorkflowSnapshot;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import com.github.h3yow.workflowdagenginejava.store.WorkflowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes one workflow instance against its persisted snapshot.
 *
 * <p>Scheduling model:
 * <ul>
 *   <li>ready steps (all deps SUCCEEDED) are launched in id order, but at most
 *       {@code maxConcurrency} steps run at the same time (semaphore);</li>
 *   <li>on the first FAILED/TIMEOUT step the workflow is marked failed: no new
 *       step is ever started, and every step that transitively depends on a
 *       non-succeeded step becomes SKIPPED; steps already in flight run to
 *       completion but their outcome is only recorded when it does not
 *       contradict the existing failure attribution;</li>
 *   <li>SUCCEEDED steps from the persisted snapshot are never executed again,
 *       which gives crash-restart recovery and idempotent replay;</li>
 *   <li>every state change is flushed via the store before scheduling moves on.</li>
 * </ul>
 */
class WorkflowDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WorkflowDispatcher.class);

    private final WorkflowStore store;
    private final StepHandlerRegistry registry;
    private final ScheduledExecutorService timeoutExecutor;
    private final long defaultTimeoutMillis;

    private final String workflowId;
    private final Map<String, StepDef> steps = new LinkedHashMap<>();
    private final Map<String, List<String>> deps = new HashMap<>();
    private final Map<String, StepRecord> records;
    private final int maxConcurrency;

    private WorkflowStatus status;
    private FailureKind failureKind;
    private String failureMessage;
    private long createdAt;

    private final Semaphore slots;
    private final List<Running> running = new ArrayList<>();
    /** Persisted step ids that finished successfully before this run (never rerun). */
    private final java.util.Set<String> alreadySucceeded = new java.util.HashSet<>();

    WorkflowDispatcher(WorkflowSnapshot initial, WorkflowStore store,
                       StepHandlerRegistry registry,
                       ScheduledExecutorService timeoutExecutor,
                       long defaultTimeoutMillis) {
        this.store = store;
        this.registry = registry;
        this.timeoutExecutor = timeoutExecutor;
        this.defaultTimeoutMillis = defaultTimeoutMillis;

        this.workflowId = initial.workflowId();
        this.maxConcurrency = Math.max(1, initial.maxConcurrency());
        for (StepDef s : initial.steps()) {
            steps.put(s.id(), s);
            deps.put(s.id(), s.dependsOn() == null ? List.of() : s.dependsOn());
        }
        this.records = new LinkedHashMap<>(initial.records());
        this.status = initial.status();
        this.failureKind = initial.failureKind();
        this.failureMessage = initial.failureMessage();
        this.createdAt = initial.createdAt();
        this.slots = new Semaphore(maxConcurrency, true);
    }

    void run() {
        logStart();

        // A step persisted as RUNNING means the previous process died mid-step:
        // reset it to PENDING so it is retried; SUCCEEDED steps stay untouched.
        for (String id : steps.keySet()) {
            StepRecord rec = records.get(id);
            if (rec != null && rec.status() == StepStatus.SUCCEEDED) {
                alreadySucceeded.add(id);
            } else if (rec != null && rec.status() == StepStatus.RUNNING) {
                records.put(id, new StepRecord(StepStatus.PENDING, "reset after interrupted execution",
                        null, rec.attempts(), 0L, 0L));
            }
        }

        status = WorkflowStatus.RUNNING;
        persist();

        PriorityQueue<String> ready = new PriorityQueue<>();
        for (String id : steps.keySet()) {
            if (records.get(id).status() == StepStatus.PENDING && depsSatisfied(id)) {
                ready.add(id);
            }
        }

        boolean failureObserved = false;
        while (true) {
            // Launch every launchable ready step while concurrency allows.
            while (!failureObserved && !ready.isEmpty() && slots.availablePermits() > 0) {
                String id = ready.poll();
                if (records.get(id).status() != StepStatus.PENDING || !depsSatisfied(id)) {
                    continue;
                }
                launch(id);
            }

            if (running.isEmpty()) {
                break;
            }
            Running finished = awaitAnyFinished();
            for (String id : collectReadyAfter(finished.id)) {
                ready.add(id);
            }
            if (finished.outcome.status != StepStatus.SUCCEEDED) {
                failureObserved = true;
                ready.clear();
                markDownstreamSkipped();
            }
        }

        finish(failureObserved);
    }

    private void launch(String id) {
        StepDef def = steps.get(id);
        slots.acquireUninterruptibly();

        int attempts = records.get(id).attempts() + 1;
        long startedAt = System.currentTimeMillis();
        records.put(id, new StepRecord(StepStatus.RUNNING, null, null, attempts, startedAt, 0L));
        persist();
        log.info("workflow [{}] step [{}] started (deps={}, attempt={})",
                workflowId, id, deps.get(id), attempts);

        StepHandler handler = registry.find(def.action())
                .orElse(null);
        long timeout = def.timeoutMillis() > 0 ? def.timeoutMillis() : defaultTimeoutMillis;

        CompletableFuture<Outcome> future = new CompletableFuture<>();
        Thread runner = Thread.ofVirtual().name("wf-step-" + workflowId + "-" + id).start(() -> {
            if (handler == null) {
                future.complete(Outcome.failure(FailureKind.STEP_EXCEPTION,
                        "no handler registered for action '" + def.action() + "'"));
                return;
            }
            try {
                String detail = handler.execute(id);
                future.complete(Outcome.success(detail == null ? "done" : detail));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                future.complete(Outcome.timeout("step timed out after " + timeout + "ms"));
            } catch (Throwable t) {
                future.complete(Outcome.failure(FailureKind.STEP_EXCEPTION,
                        t.getClass().getSimpleName() + ": " + t.getMessage()));
            }
        });

        // Guaranteed TIMEOUT attribution even if the handler swallows interrupt:
        // the timeout action wins the completion race.
        Future<?> timeoutTicket = timeoutExecutor.schedule(() -> {
            if (future.complete(Outcome.timeout("step timed out after " + timeout + "ms"))) {
                runner.interrupt();
            }
        }, timeout, TimeUnit.MILLISECONDS);

        future.whenComplete((o, ex) -> timeoutTicket.cancel(false));
        running.add(new Running(id, future, timeoutTicket));
    }

    private Running awaitAnyFinished() {
        while (true) {
            for (Running r : running) {
                if (r.future.isDone()) {
                    running.remove(r);
                    slots.release();
                    r.timeoutTicket.cancel(false);
                    Outcome outcome = safeGet(r.future);
                    r.outcome = outcome;
                    recordOutcome(r.id, outcome);
                    return r;
                }
            }
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("dispatcher interrupted", e);
            }
        }
    }

    private Outcome safeGet(CompletableFuture<Outcome> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Outcome.failure(FailureKind.STEP_EXCEPTION, "dispatcher interrupted");
        } catch (ExecutionException e) {
            return Outcome.failure(FailureKind.STEP_EXCEPTION, String.valueOf(e.getCause()));
        }
    }

    private void recordOutcome(String id, Outcome outcome) {
        long finishedAt = System.currentTimeMillis();
        StepRecord current = records.get(id);
        StepRecord next;
        switch (outcome.status) {
            case SUCCEEDED -> {
                next = new StepRecord(StepStatus.SUCCEEDED, outcome.message, null,
                        current.attempts(), current.startedAt(), finishedAt);
                log.info("workflow [{}] step [{}] SUCCEEDED: {}", workflowId, id, outcome.message);
            }
            case TIMEOUT -> {
                next = new StepRecord(StepStatus.TIMEOUT, outcome.message, FailureKind.TIMEOUT,
                        current.attempts(), current.startedAt(), finishedAt);
                log.warn("workflow [{}] step [{}] TIMEOUT: {}", workflowId, id, outcome.message);
                if (failureKind == null) {
                    failureKind = FailureKind.TIMEOUT;
                    failureMessage = "step '" + id + "' timed out";
                }
            }
            default -> {
                next = new StepRecord(StepStatus.FAILED, outcome.message, FailureKind.STEP_EXCEPTION,
                        current.attempts(), current.startedAt(), finishedAt);
                log.warn("workflow [{}] step [{}] FAILED: {}", workflowId, id, outcome.message);
                if (failureKind == null) {
                    failureKind = FailureKind.STEP_EXCEPTION;
                    failureMessage = "step '" + id + "' failed: " + outcome.message;
                }
            }
        }
        records.put(id, next);
        persist();
    }

    /** After {@code changedId} finishes, return pending steps whose deps are now all succeeded. */
    private List<String> collectReadyAfter(String changedId) {
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, List<String>> e : deps.entrySet()) {
            String id = e.getKey();
            if (records.get(id).status() == StepStatus.PENDING
                    && e.getValue().contains(changedId)
                    && depsSatisfied(id)) {
                out.add(id);
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    private boolean depsSatisfied(String id) {
        for (String dep : deps.get(id)) {
            if (records.get(dep).status() != StepStatus.SUCCEEDED) {
                return false;
            }
        }
        return true;
    }

    /**
     * Skip every pending step that (transitively) depends on a step that did
     * not succeed. Idempotent: already-terminal steps are left alone.
     */
    private void markDownstreamSkipped() {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (String id : steps.keySet()) {
                StepRecord rec = records.get(id);
                if (rec.status() != StepStatus.PENDING && rec.status() != StepStatus.RUNNING) {
                    continue;
                }
                boolean blockedByFailure = false;
                for (String dep : deps.get(id)) {
                    StepStatus ds = records.get(dep).status();
                    if (ds == StepStatus.FAILED || ds == StepStatus.TIMEOUT || ds == StepStatus.SKIPPED) {
                        blockedByFailure = true;
                        break;
                    }
                }
                if (rec.status() == StepStatus.PENDING && blockedByFailure) {
                    records.put(id, new StepRecord(StepStatus.SKIPPED,
                            "skipped due to upstream failure", FailureKind.STEP_EXCEPTION,
                            rec.attempts(), 0L, System.currentTimeMillis()));
                    log.info("workflow [{}] step [{}] SKIPPED due to upstream failure", workflowId, id);
                    changed = true;
                }
            }
        }
        persist();
    }

    private void finish(boolean failureObserved) {
        boolean allSucceeded = records.values().stream()
                .allMatch(r -> r.status() == StepStatus.SUCCEEDED);
        if (!failureObserved && allSucceeded) {
            status = WorkflowStatus.SUCCEEDED;
            failureKind = null;
            failureMessage = null;
        } else {
            status = WorkflowStatus.FAILED;
            if (failureKind == null) {
                failureKind = FailureKind.STEP_EXCEPTION;
                failureMessage = "workflow ended with unfinished steps";
            }
        }
        persist();
        logResult();
    }

    private void persist() {
        long now = System.currentTimeMillis();
        WorkflowSnapshot snap = new WorkflowSnapshot(
                workflowId, status, maxConcurrency, List.copyOf(steps.values()),
                Map.copyOf(records), failureKind, failureMessage, createdAt, now);
        store.save(snap);
    }

    private void logStart() {
        StringBuilder sb = new StringBuilder();
        for (StepDef s : steps.values()) {
            sb.append("\n    ").append(s.id())
              .append(" action=").append(s.action())
              .append(" dependsOn=").append(s.dependsOn())
              .append(" timeout=").append(s.timeoutMillis());
        }
        log.info("workflow [{}] starting/resuming, status={}, maxConcurrency={}, steps:{}",
                workflowId, status, maxConcurrency, sb);
    }

    private void logResult() {
        StringBuilder sb = new StringBuilder();
        records.forEach((id, rec) -> sb.append("\n    ").append(id).append('=').append(rec.status()));
        log.info("workflow [{}] final status={} failureKind={} message={}, step states:{}",
                workflowId, status, failureKind, failureMessage, sb);
    }

    private record Outcome(StepStatus status, String message) {
        static Outcome success(String msg) {
            return new Outcome(StepStatus.SUCCEEDED, msg);
        }

        static Outcome failure(FailureKind kind, String msg) {
            StepStatus st = kind == FailureKind.TIMEOUT ? StepStatus.TIMEOUT : StepStatus.FAILED;
            return new Outcome(st, msg);
        }

        static Outcome timeout(String msg) {
            return new Outcome(StepStatus.TIMEOUT, msg);
        }
    }

    private static final class Running {
        final String id;
        final CompletableFuture<Outcome> future;
        final Future<?> timeoutTicket;
        volatile Outcome outcome;

        Running(String id, CompletableFuture<Outcome> future, Future<?> timeoutTicket) {
            this.id = id;
            this.future = future;
            this.timeoutTicket = timeoutTicket;
        }
    }
}
