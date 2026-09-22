package com.github.h3yow.workflowdagenginejava.tests.engine;

import com.github.h3yow.workflowdagenginejava.handler.StepHandler;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Shared helpers for engine tests: temp data dirs, waiting and test handlers. */
final class TestInfrastructure {

    private TestInfrastructure() {
    }

    static Path newDataDir() {
        try {
            return Files.createTempDirectory("wf-tests-");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void deleteRecursively(Path dir) {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (Exception ignored) {
                    // best effort cleanup
                }
            });
        } catch (Exception ignored) {
            // best effort cleanup
        }
    }

    /** Poll until predicate passes or the timeout elapses. */
    static void awaitUntil(Duration timeout, java.util.function.Supplier<Boolean> condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        AssertionError last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (Boolean.TRUE.equals(condition.get())) {
                    return;
                }
            } catch (AssertionError e) {
                last = e;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        if (last != null) {
            throw last;
        }
        throw new AssertionError("condition not met within " + timeout);
    }

    /** Counts how many times it executes; appends to a trace list per execution. */
    static final class CountingHandler implements StepHandler {
        private final String action;
        private final long sleepMillis;
        final AtomicInteger counter = new AtomicInteger();
        final List<String> trace;

        CountingHandler(String action, long sleepMillis, List<String> trace) {
            this.action = action;
            this.sleepMillis = sleepMillis;
            this.trace = trace;
        }

        @Override
        public String action() {
            return action;
        }

        @Override
        public String execute(String stepId) throws Exception {
            counter.incrementAndGet();
            trace.add(stepId + "@" + counter.get());
            if (sleepMillis > 0) {
                Thread.sleep(sleepMillis);
            }
            return "ran-" + counter.get();
        }
    }

    /** Throws every time it runs. */
    static final class AlwaysFailHandler implements StepHandler {
        private final String action;

        AlwaysFailHandler(String action) {
            this.action = action;
        }

        @Override
        public String action() {
            return action;
        }

        @Override
        public String execute(String stepId) {
            throw new IllegalStateException("boom from " + stepId);
        }
    }
}
