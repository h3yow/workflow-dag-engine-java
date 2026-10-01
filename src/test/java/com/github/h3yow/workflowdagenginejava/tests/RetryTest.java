package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.model.AttemptResult;
import com.github.h3yow.workflowdagenginejava.model.StepAttempt;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.StepRun;
import com.github.h3yow.workflowdagenginejava.model.StepStatus;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 自动重试测试：
 * 前几次失败最后一次成功；次数耗尽才算 FAILED；超时也走重试；
 * 每次尝试都有独立耗时与结果明细；退避等待期间仍占用并发槽位，不突破并发上限。
 */
class RetryTest {

    @TempDir
    Path dataDir;

    @Test
    void retriesThenSucceedsAndRecordsEveryAttempt() {
        AtomicInteger calls = new AtomicInteger();
        StepHandler flaky = (wf, stepId, param) -> {
            if (calls.incrementAndGet() <= 2) {
                throw new IllegalStateException("transient-" + calls.get());
            }
        };
        List<StepDef> steps = List.of(
                TestSupport.retryStep("r", "flaky", null, 0L, 2, 0L),
                TestSupport.noop("after", "r"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000, Map.of("flaky", flaky));
        engine.submit(new WorkflowDef("wf-retry-ok", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-ok");
        TestSupport.trace(exec);

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        StepRun r = exec.getSteps().get("r");
        assertEquals(StepStatus.SUCCESS, r.getStatus());
        assertEquals(3, calls.get(), "前两次失败 + 第三次成功，共执行 3 次");
        assertEquals(3, r.getAttempts());
        assertEquals(3, r.getAttemptRecords().size(), "每次尝试都要有明细");
        assertEquals(AttemptResult.STEP_EXCEPTION, r.getAttemptRecords().get(0).result());
        assertEquals(AttemptResult.STEP_EXCEPTION, r.getAttemptRecords().get(1).result());
        assertEquals(AttemptResult.SUCCESS, r.getAttemptRecords().get(2).result());
        assertTrue(r.getAttemptRecords().get(0).message().contains("transient-1"));
        // 每次尝试都记录了耗时
        for (StepAttempt a : r.getAttemptRecords()) {
            assertTrue(a.finishedAt() >= a.startedAt(), "尝试 " + a.attemptNo() + " 耗时异常");
            assertEquals(1, a.runNo(), "首次执行批次为 1");
        }
        assertEquals(1, r.getAttemptRecords().get(0).attemptNo());
        assertEquals(3, r.getAttemptRecords().get(2).attemptNo());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("after").getStatus());
    }

    @Test
    void exhaustedRetriesFailStepWithLastFailureTypeAndSkipDownstream() {
        AtomicInteger calls = new AtomicInteger();
        StepHandler alwaysFail = (wf, stepId, param) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("permanent");
        };
        List<StepDef> steps = List.of(
                TestSupport.retryStep("r", "alwaysFail", null, 0L, 2, 0L),
                TestSupport.noop("down", "r"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("alwaysFail", alwaysFail));
        engine.submit(new WorkflowDef("wf-retry-exhaust", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-exhaust");
        TestSupport.trace(exec);

        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        StepRun r = exec.getSteps().get("r");
        assertEquals(StepStatus.FAILED, r.getStatus());
        assertEquals(3, calls.get(), "1 次初始尝试 + 2 次重试，全部执行");
        assertEquals(3, r.getAttempts());
        assertEquals(3, r.getAttemptRecords().size());
        assertEquals(AttemptResult.STEP_EXCEPTION, r.getAttemptRecords().get(2).result());
        assertNotNull(r.getFailure());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.FailureType.STEP_EXCEPTION,
                r.getFailure().getFailureType(), "最终失败原因要能区分类型");
        assertEquals("r", exec.getFailure().getStepId());
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("down").getStatus());
    }

    @Test
    void timeoutAlsoTriggersRetryAndEachAttemptHasOwnTimeoutBudget() {
        long start = System.currentTimeMillis();
        // sleep 500ms，单次超时 80ms，配 1 次重试：两次都超时，总耗时约 160ms 而不是 500ms
        List<StepDef> steps = List.of(
                TestSupport.retryStep("slow", "sleep", "500", 80L, 1, 0L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-retry-timeout", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-timeout");
        TestSupport.trace(exec);

        StepRun slow = exec.getSteps().get("slow");
        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        assertEquals(StepStatus.FAILED, slow.getStatus());
        assertEquals(2, slow.getAttempts());
        assertEquals(2, slow.getAttemptRecords().size());
        assertEquals(AttemptResult.TIMEOUT, slow.getAttemptRecords().get(0).result());
        assertEquals(AttemptResult.TIMEOUT, slow.getAttemptRecords().get(1).result());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.FailureType.TIMEOUT,
                slow.getFailure().getFailureType());
        TestSupport.assertDurationBetween(start, 150, 450);
    }

    @Test
    void retryBackoffHoldsConcurrencySlot() throws Exception {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        StepHandler observe = (wf, stepId, param) -> {
            int cur = running.incrementAndGet();
            maxObserved.accumulateAndGet(cur, Math::max);
            Thread.sleep(60);
            running.decrementAndGet();
            throw new IllegalStateException("fail-for-retry");
        };
        // 并发上限 2：两个步骤各自重试（1 次重试 + 100ms 退避），
        // 退避期间槽位不释放，因此任何时刻在跑步骤数不得超过 2
        List<StepDef> steps = List.of(
                TestSupport.retryStep("x", "observe", null, 5000L, 1, 100L),
                TestSupport.retryStep("y", "observe", null, 5000L, 1, 100L),
                TestSupport.step("z", "observe", null, 5000L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of("observe", observe));
        long start = System.currentTimeMillis();
        engine.submit(new WorkflowDef("wf-retry-cap", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-cap");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        assertTrue(maxObserved.get() <= 2,
                "重试与退避等待都不得突破并发上限，观测峰值=" + maxObserved.get());
        // x/y：两次尝试 + 100ms 退避 ≈ 220ms；z 必须等槽位释放后才能跑，总耗时明显大于 200ms
        TestSupport.assertDurationBetween(start, 200, 2000);
        assertEquals(2, exec.getSteps().get("x").getAttempts());
    }

    @Test
    void noRetryConfigBehavesExactlyAsBefore() {
        AtomicInteger calls = new AtomicInteger();
        StepHandler failOnce = (wf, stepId, param) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("boom");
        };
        List<StepDef> steps = List.of(
                TestSupport.step("f", "failOnce", null, 0L),
                TestSupport.noop("g", "f"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000, Map.of("failOnce", failOnce));
        engine.submit(new WorkflowDef("wf-no-retry", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-no-retry");
        TestSupport.trace(exec);

        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        assertEquals(1, calls.get(), "没有重试配置的步骤只能执行一次");
        StepRun f = exec.getSteps().get("f");
        assertEquals(1, f.getAttempts());
        assertEquals(1, f.getAttemptRecords().size());
        assertEquals(AttemptResult.STEP_EXCEPTION, f.getAttemptRecords().get(0).result());
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("g").getStatus());
    }
}
