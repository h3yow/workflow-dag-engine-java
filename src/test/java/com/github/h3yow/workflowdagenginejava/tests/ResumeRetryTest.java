package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.SubmitOutcome;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 续跑与自动重试的组合语义测试：
 * 续跑批次里每个被重跑的步骤重新获得定义里配置的全部重试次数；
 * 这一轮重试用尽才算真正失败并按既有规则停掉下游；
 * 并发续跑的所有请求拿到同一次续跑的一致终态结果；
 * 续跑批次中途重启可断点恢复，重试预算、尝试次数与尝试记录如实保留。
 */
class ResumeRetryTest {

    @TempDir
    Path dataDir;

    /** 前 failTimes 次调用抛异常、之后成功的处理器。 */
    private static StepHandler failTimesThenOk(int failTimes, AtomicInteger calls) {
        return (wf, stepId, param) -> {
            if (calls.incrementAndGet() <= failTimes) {
                throw new IllegalStateException("第 " + calls.get() + " 次调用故意失败");
            }
        };
    }

    @Test
    void resumeGrantsFreshRetryBudgetAndEventuallySucceeds() {
        // maxRetries=2：首轮 3 次尝试用尽后 FAILED；续跑轮再失败 1 次后成功
        AtomicInteger bCalls = new AtomicInteger();
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "flakyB", null, 0L, 2, 20L, "a"),
                TestSupport.noop("c", "b"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flakyB", failTimesThenOk(4, bCalls)));
        engine.submit(new WorkflowDef("wf-resume-retry-ok", steps));

        WorkflowExecution failed = TestSupport.awaitTerminal(engine, "wf-resume-retry-ok");
        assertEquals(WorkflowStatus.FAILED, failed.getStatus());
        assertEquals(3, failed.getSteps().get("b").getAttempts(), "首轮 1+2 次尝试用尽才失败");

        SubmitOutcome outcome = engine.resume("wf-resume-retry-ok");
        assertFalse(outcome.deduplicated());
        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-retry-ok");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus(),
                "续跑轮重新获得重试次数，重试到成功应与正常执行表现一致");
        StepRun b = exec.getSteps().get("b");
        assertEquals(StepStatus.SUCCESS, b.getStatus());
        assertEquals(5, b.getAttempts(), "首轮 3 次 + 续跑轮 2 次，历史次数保留不累加也不丢");
        assertEquals(5, bCalls.get());
        assertNull(b.getFailure(), "最终成功后失败信息应清除");
        List<AttemptRecord> records = b.getAttemptRecords();
        assertEquals(5, records.size(), "两轮的每次尝试都要有记录");
        for (int i = 0; i < records.size(); i++) {
            assertEquals(i + 1, records.get(i).getAttemptNo(), "尝试编号连续、不重复");
            assertTrue(records.get(i).getFinishedAt() > 0, "每条记录都有结果时间");
        }
        assertTrue(records.get(3).getFailureType() == FailureType.STEP_EXCEPTION,
                "续跑轮的第一次失败也如实记录");
        assertTrue(records.get(4).isSuccess());
        assertEquals(1, exec.getSteps().get("a").getAttempts(), "已成功的 a 绝不重跑");
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("c").getStatus());
    }

    @Test
    void resumeRetryExhaustedAgainCountsAsRealFailure() {
        // 处理器永远失败：续跑轮必须把 1+maxRetries 次也用尽才算真正失败
        AtomicInteger bCalls = new AtomicInteger();
        StepHandler alwaysFail = (wf, stepId, param) -> {
            bCalls.incrementAndGet();
            throw new IllegalStateException("永远失败");
        };
        List<StepDef> steps = List.of(
                TestSupport.step("b", "alwaysFail", null, 0L, 2, 20L),
                TestSupport.noop("child", "b"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("alwaysFail", alwaysFail));
        engine.submit(new WorkflowDef("wf-resume-retry-exhausted", steps));

        WorkflowExecution firstRound = TestSupport.awaitTerminal(engine, "wf-resume-retry-exhausted");
        assertEquals(WorkflowStatus.FAILED, firstRound.getStatus());
        assertEquals(3, firstRound.getSteps().get("b").getAttempts());

        engine.resume("wf-resume-retry-exhausted");
        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-retry-exhausted");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.FAILED, exec.getStatus(), "续跑轮重试也用尽，工作流再次失败");
        StepRun b = exec.getSteps().get("b");
        assertEquals(StepStatus.FAILED, b.getStatus());
        assertEquals(6, b.getAttempts(),
                "续跑轮完整重试 3 次后才算真正失败（若只跑 1 次就失败说明重试预算被吞）");
        assertEquals(6, bCalls.get());
        assertEquals(6, b.getAttemptRecords().size());
        assertTrue(b.getAttemptRecords().stream().noneMatch(AttemptRecord::isSuccess));
        assertEquals(FailureType.STEP_EXCEPTION, b.getFailure().getFailureType());
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("child").getStatus(),
                "续跑轮真正失败后仍按既有规则停掉下游");
        assertEquals(0, exec.getSteps().get("child").getAttempts(), "下游从头到尾没执行过");
    }

    @Test
    void concurrentResumeAllRequestsGetConsistentTerminalResult() throws Exception {
        // 续跑轮的 b 先睡 300ms 再成功，保证并发续跑请求到达时批次仍在执行
        AtomicInteger bCalls = new AtomicInteger();
        StepHandler flakySlow = (wf, stepId, param) -> {
            int call = bCalls.incrementAndGet();
            if (call == 1) {
                throw new IllegalStateException("首轮故意失败");
            }
            Thread.sleep(300);
        };
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "flakySlow", null, 0L, "a"),
                TestSupport.noop("c", "b"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("flakySlow", flakySlow));
        engine.submit(new WorkflowDef("wf-resume-consistent", steps));
        assertEquals(WorkflowStatus.FAILED,
                TestSupport.awaitTerminal(engine, "wf-resume-consistent").getStatus());

        int threads = 8;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<SubmitOutcome>> futures = new ArrayList<>();
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                go.await();
                return engine.resume("wf-resume-consistent");
            }));
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();

        int accepted = 0;
        List<WorkflowExecution> dedupResults = new ArrayList<>();
        for (Future<SubmitOutcome> f : futures) {
            SubmitOutcome outcome = f.get(30, TimeUnit.SECONDS);
            if (outcome.deduplicated()) {
                dedupResults.add(outcome.execution());
            } else {
                accepted++;
            }
        }
        pool.shutdown();

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-consistent");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(1, accepted, "并发续跑只能有一个真正受理");
        assertEquals(threads - 1, dedupResults.size());
        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        for (WorkflowExecution result : dedupResults) {
            assertEquals(WorkflowStatus.SUCCESS, result.getStatus(),
                    "幂等命中的请求必须拿到同一次续跑的终态结果，而不是执行中的半截快照");
            assertEquals(exec.getUpdatedAt(), result.getUpdatedAt(),
                    "所有请求看到的是同一份终态快照");
            assertEquals(2, result.getSteps().get("b").getAttempts());
            assertEquals(StepStatus.SUCCESS, result.getSteps().get("c").getStatus());
        }
        assertEquals(2, bCalls.get(), "步骤 b 总共只执行两次，并发续跑不允许多跑");
    }

    @Test
    void resumeRetryBatchSurvivesRestartWithoutLosingAttempts() throws Exception {
        // maxRetries=2：首轮 3 次用尽失败；续跑轮第 1 次尝试（总第 4 次）执行中被"崩溃"打断，
        // 恢复后该步骤用续跑批次剩余的预算重试并成功
        AtomicInteger bCalls = new AtomicInteger();
        StepHandler flakyCrashable = (wf, stepId, param) -> {
            int call = bCalls.incrementAndGet();
            if (call <= 3) {
                throw new IllegalStateException("首轮第 " + call + " 次故意失败");
            }
            if (call == 4) {
                // 续跑轮第一次尝试：执行中被"崩溃"打断。真实崩溃里线程随进程消失，
                // 测试里用中断模拟：被中断后静默返回，不再触碰任何共享状态
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                throw new IllegalStateException("这次尝试被崩溃打断（结果不可信）");
            }
        };
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "flakyCrashable", null, 0L, 2, 20L, "a"),
                TestSupport.noop("c", "b"));

        WorkflowEngine first = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flakyCrashable", flakyCrashable));
        first.submit(new WorkflowDef("wf-resume-retry-restart", steps));
        assertEquals(WorkflowStatus.FAILED,
                TestSupport.awaitTerminal(first, "wf-resume-retry-restart").getStatus());
        first.resume("wf-resume-retry-restart");
        TestSupport.waitUntilStep(first, "wf-resume-retry-restart", "b", StepStatus.RUNNING);
        // 等第 4 次调用的处理器真正进入执行，再"崩溃"——否则崩溃可能发生在处理器启动前
        long deadline = System.currentTimeMillis() + 10_000;
        while (bCalls.get() < 4 && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(4, bCalls.get(), "崩溃前续跑轮的第一次尝试应已进入处理器");
        first.shutdown(); // 模拟进程在续跑批次中途突然终止

        WorkflowEngine second = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flakyCrashable", flakyCrashable));
        second.recoverOnStartup();
        WorkflowExecution exec = TestSupport.awaitTerminal(second, "wf-resume-retry-restart");
        TestSupport.trace(exec);
        second.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus(),
                "续跑批次重启后接着跑完，剩余重试预算仍然有效");
        StepRun b = exec.getSteps().get("b");
        assertEquals(5, b.getAttempts(), "首轮 3 次 + 续跑轮被中断 1 次 + 恢复后成功 1 次");
        assertEquals(5, bCalls.get(), "已完成的尝试不重复执行，次数不重复累加");
        List<AttemptRecord> records = b.getAttemptRecords();
        assertEquals(5, records.size(), "每次尝试的记录如实保留，不丢不重复");
        for (int i = 0; i < records.size(); i++) {
            assertEquals(i + 1, records.get(i).getAttemptNo());
            assertTrue(records.get(i).getFinishedAt() > 0, "被中断的尝试也被补记了结束时间");
        }
        assertFalse(records.get(3).isSuccess());
        assertTrue(records.get(3).getMessage().contains("中断"), "崩溃打断的尝试有如实的中断记录");
        assertTrue(records.get(4).isSuccess());
        assertEquals(1, exec.getSteps().get("a").getAttempts(), "首轮就成功的 a 全程只跑一次");
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("c").getStatus());
        // 恢复后整体状态与每步结果都能正常查询
        WorkflowExecution queried = second.get("wf-resume-retry-restart").orElseThrow();
        assertEquals(WorkflowStatus.SUCCESS, queried.getStatus());
        assertEquals(5, queried.getSteps().get("b").getAttemptRecords().size());
    }
}
