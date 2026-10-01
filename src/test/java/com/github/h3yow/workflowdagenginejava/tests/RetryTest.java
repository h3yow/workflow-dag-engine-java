package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.model.AttemptRecord;
import com.github.h3yow.workflowdagenginejava.model.FailureType;
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
 * 步骤自动重试测试：
 * 内部报错/超时按配置自动重试，次数用尽才算最终失败；
 * 每次尝试的次数、耗时与结果都有记录；重试不突破并发上限；
 * 未配置重试的老定义行为不变。
 */
class RetryTest {

    @TempDir
    Path dataDir;

    @Test
    void retrySucceedsWithinAllowedAttempts() {
        AtomicInteger calls = new AtomicInteger();
        StepHandler flaky = (wf, stepId, param) -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("第 " + calls.get() + " 次故意失败");
            }
        };
        List<StepDef> steps = List.of(
                TestSupport.step("flaky", "flaky", null, 0L, 2, 50L),
                TestSupport.noop("after", "flaky"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of("flaky", flaky));
        engine.submit(new WorkflowDef("wf-retry-ok", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-ok");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        StepRun run = exec.getSteps().get("flaky");
        assertEquals(StepStatus.SUCCESS, run.getStatus());
        assertEquals(3, run.getAttempts(), "前两次失败 + 第三次成功，共 3 次尝试");
        assertEquals(3, calls.get(), "处理器确实被调用了 3 次");
        assertNull(run.getFailure(), "最终成功的步骤不应残留失败信息");

        List<AttemptRecord> records = run.getAttemptRecords();
        assertEquals(3, records.size(), "每次尝试都要有溯源记录");
        for (int i = 0; i < records.size(); i++) {
            AttemptRecord r = records.get(i);
            assertEquals(i + 1, r.getAttemptNo());
            assertTrue(r.getStartedAt() > 0 && r.getFinishedAt() >= r.getStartedAt(),
                    "第 " + (i + 1) + " 次尝试的起止时间必须如实记录");
        }
        assertFalse(records.get(0).isSuccess());
        assertEquals(FailureType.STEP_EXCEPTION, records.get(0).getFailureType());
        assertFalse(records.get(1).isSuccess());
        assertTrue(records.get(2).isSuccess());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("after").getStatus());
    }

    @Test
    void retryExhaustedThenStepFailsAndDownstreamSkipped() {
        List<StepDef> steps = List.of(
                TestSupport.step("bad", "fail", "boom", 0L, 2, 30L),
                TestSupport.noop("child", "bad"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-retry-exhausted", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-exhausted");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        StepRun bad = exec.getSteps().get("bad");
        assertEquals(StepStatus.FAILED, bad.getStatus());
        assertEquals(3, bad.getAttempts(), "1 次首发 + 2 次重试，次数用尽才算失败");
        assertEquals(FailureType.STEP_EXCEPTION, bad.getFailure().getFailureType());
        assertEquals(3, bad.getAttemptRecords().size());
        assertTrue(bad.getAttemptRecords().stream().noneMatch(AttemptRecord::isSuccess));
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("child").getStatus(),
                "重试耗尽后的最终失败仍按既有规则停掉下游");
    }

    @Test
    void timeoutAlsoTriggersRetry() {
        long start = System.currentTimeMillis();
        List<StepDef> steps = List.of(
                TestSupport.step("slow", "sleep", "1000", 100L, 1, 20L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 1, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-retry-timeout", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-timeout");
        TestSupport.trace(exec);
        engine.shutdown();

        StepRun slow = exec.getSteps().get("slow");
        assertEquals(StepStatus.FAILED, slow.getStatus());
        assertEquals(2, slow.getAttempts(), "超时同样计入尝试并触发重试");
        assertEquals(FailureType.TIMEOUT, slow.getFailure().getFailureType());
        assertEquals(2, slow.getAttemptRecords().size());
        assertTrue(slow.getAttemptRecords().stream()
                .allMatch(r -> r.getFailureType() == FailureType.TIMEOUT));
        // 两次尝试各约 100ms 超时即返回，总耗时应远小于串行跑完两次 1000ms
        TestSupport.assertDurationBetween(start, 200, 1500);
    }

    @Test
    void retryRespectsConcurrencyCap() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        StepHandler busyFlaky = (wf, stepId, param) -> {
            int cur = running.incrementAndGet();
            maxObserved.accumulateAndGet(cur, Math::max);
            try {
                Thread.sleep(100);
                if (calls.incrementAndGet() <= 2) {
                    throw new IllegalStateException("故意失败以触发重试");
                }
            } finally {
                running.decrementAndGet();
            }
        };
        List<StepDef> steps = List.of(
                TestSupport.step("x", "busyFlaky", null, 0L, 1, 10L),
                TestSupport.step("y", "busyFlaky", null, 0L, 1, 10L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 1, 5000,
                Map.of("busyFlaky", busyFlaky));
        engine.submit(new WorkflowDef("wf-retry-cap", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-retry-cap");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        assertTrue(maxObserved.get() <= 1,
                "重试派发不得突破并发上限，观测到最大并发 " + maxObserved.get());
        assertEquals(2, exec.getSteps().get("x").getAttempts());
        assertEquals(2, exec.getSteps().get("y").getAttempts());
    }

    @Test
    void definitionWithoutRetryKeepsLegacyBehavior() {
        List<StepDef> steps = List.of(
                TestSupport.step("bad", "fail", "boom", 0L),
                TestSupport.noop("child", "bad"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-no-retry", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-no-retry");
        TestSupport.trace(exec);
        engine.shutdown();

        StepRun bad = exec.getSteps().get("bad");
        assertEquals(StepStatus.FAILED, bad.getStatus());
        assertEquals(1, bad.getAttempts(), "未配置重试的老定义只执行一次，行为不变");
        assertEquals(1, bad.getAttemptRecords().size());
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("child").getStatus());
    }
}
