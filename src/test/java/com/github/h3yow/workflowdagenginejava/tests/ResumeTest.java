package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.SubmitOutcome;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowNotFoundException;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.StepStatus;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败工作流续跑测试：
 * 只补跑未成功步骤、已成功步骤绝不重跑；并发续跑只生效一次；
 * 续跑中途重启可断点恢复且不重复执行；已成功/未知工作流的续跑语义明确。
 */
class ResumeTest {

    @TempDir
    Path dataDir;

    /** 第一次调用失败、之后成功的处理器，配合计数验证"只跑一次"。 */
    private static StepHandler failOnce(AtomicInteger calls) {
        return (wf, stepId, param) -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("首次执行故意失败");
            }
        };
    }

    @Test
    void resumeRerunsOnlyUnsuccessfulSteps() {
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger aCalls = new AtomicInteger();
        StepHandler countA = (wf, stepId, param) -> aCalls.incrementAndGet();
        List<StepDef> steps = List.of(
                TestSupport.step("a", "countA", null, 0L),
                TestSupport.step("b", "flakyB", null, 0L, "a"),
                TestSupport.noop("c", "b"),
                TestSupport.noop("d", "c"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("countA", countA, "flakyB", failOnce(bCalls)));
        engine.submit(new WorkflowDef("wf-resume", steps));

        WorkflowExecution failed = TestSupport.awaitTerminal(engine, "wf-resume");
        assertEquals(WorkflowStatus.FAILED, failed.getStatus());
        assertEquals(StepStatus.SKIPPED, failed.getSteps().get("c").getStatus());

        SubmitOutcome outcome = engine.resume("wf-resume");
        assertFalse(outcome.deduplicated(), "首次续跑应当真正受理");
        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        assertEquals(1, aCalls.get(), "已成功的步骤 a 绝不允许重跑");
        assertEquals(1, exec.getSteps().get("a").getAttempts());
        assertEquals(2, bCalls.get(), "失败步骤 b 只补跑一次");
        assertEquals(2, exec.getSteps().get("b").getAttempts());
        assertEquals(2, exec.getSteps().get("b").getAttemptRecords().size(),
                "续跑前的尝试记录保留、续跑的尝试追加，不重复累加");
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("c").getStatus());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("d").getStatus());
        assertEquals(1, exec.getSteps().get("c").getAttempts());
        assertNull(exec.getFailure(), "续跑成功后整体失败信息应清除");
    }

    @Test
    void concurrentResumeTriggersOnlyOneExecution() throws Exception {
        AtomicInteger bCalls = new AtomicInteger();
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "flakyB", null, 0L, "a"),
                TestSupport.noop("c", "b"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("flakyB", failOnce(bCalls)));
        engine.submit(new WorkflowDef("wf-resume-race", steps));
        assertEquals(WorkflowStatus.FAILED,
                TestSupport.awaitTerminal(engine, "wf-resume-race").getStatus());

        int threads = 8;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<SubmitOutcome>> futures = new ArrayList<>();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                return engine.resume("wf-resume-race");
            }));
        }
        assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
        go.countDown();

        int accepted = 0;
        for (Future<SubmitOutcome> f : futures) {
            SubmitOutcome outcome = f.get(30, java.util.concurrent.TimeUnit.SECONDS);
            if (!outcome.deduplicated()) {
                accepted++;
            }
        }
        pool.shutdown();

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-race");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(1, accepted, "并发续跑只能有一个真正受理，其余幂等命中同一份执行");
        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        assertEquals(2, bCalls.get(), "步骤 b 总共只执行两次（首发失败 + 续跑补跑），不允许多跑");
        assertEquals(2, exec.getSteps().get("b").getAttempts());
    }

    @Test
    void resumeOfSucceededWorkflowIsDeduplicatedNoop() {
        AtomicInteger calls = new AtomicInteger();
        StepHandler count = (wf, stepId, param) -> calls.incrementAndGet();
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of("count", count));
        engine.submit(new WorkflowDef("wf-resume-success",
                List.of(TestSupport.step("a", "count", null, 0L))));
        assertEquals(WorkflowStatus.SUCCESS,
                TestSupport.awaitTerminal(engine, "wf-resume-success").getStatus());

        SubmitOutcome outcome = engine.resume("wf-resume-success");
        assertTrue(outcome.deduplicated());
        assertEquals(WorkflowStatus.SUCCESS, outcome.execution().getStatus());
        engine.shutdown();
        assertEquals(1, calls.get(), "已成功的工作流续跑不得产生任何重复执行");
    }

    @Test
    void resumeOfUnknownWorkflowRejected() {
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        assertThrows(WorkflowNotFoundException.class, () -> engine.resume("no-such-wf"));
        engine.shutdown();
    }

    @Test
    void resumeSurvivesRestartMidwayWithoutRerunningSucceeded() throws Exception {
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger cCalls = new AtomicInteger();
        StepHandler countC = (wf, stepId, param) -> {
            cCalls.incrementAndGet();
            Thread.sleep(400);
        };
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "flakyB", null, 0L, "a"),
                TestSupport.step("c", "countC", null, 0L, "b"),
                TestSupport.noop("d", "c"));

        // 第一次"进程"：失败后发起续跑，续跑进行到 c 执行中时"崩溃"
        WorkflowEngine first = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flakyB", failOnce(bCalls), "countC", countC));
        first.submit(new WorkflowDef("wf-resume-restart", steps));
        assertEquals(WorkflowStatus.FAILED,
                TestSupport.awaitTerminal(first, "wf-resume-restart").getStatus());
        first.resume("wf-resume-restart");
        TestSupport.waitUntilStep(first, "wf-resume-restart", "c", StepStatus.RUNNING);
        first.shutdown(); // 模拟进程突然终止

        // 第二次"进程"：断点恢复，续跑应当接着跑完
        WorkflowEngine second = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flakyB", failOnce(bCalls), "countC", countC));
        second.recoverOnStartup();
        WorkflowExecution exec = TestSupport.awaitTerminal(second, "wf-resume-restart");
        TestSupport.trace(exec);
        second.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        assertEquals(2, bCalls.get(), "续跑已成功补跑的 b 在重启后不得重跑");
        assertEquals(2, exec.getSteps().get("b").getAttempts());
        assertEquals(1, exec.getSteps().get("a").getAttempts(),
                "首次执行就成功的 a 全程只跑一次");
        assertEquals(2, cCalls.get(), "崩溃时中断的 c 重新执行一次，总共两次调用");
        assertEquals(2, exec.getSteps().get("c").getAttempts());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("d").getStatus());
        // 被中断的那次尝试有如实记录
        var cRecords = exec.getSteps().get("c").getAttemptRecords();
        assertEquals(2, cRecords.size());
        assertFalse(cRecords.get(0).isSuccess());
        assertTrue(cRecords.get(1).isSuccess());
    }
}
