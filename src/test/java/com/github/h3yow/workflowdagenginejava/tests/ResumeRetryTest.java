package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.SubmitOutcome;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.model.AttemptRecord;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 续跑批次里的自动重试测试：
 * 续跑时每个被重跑的步骤重新获得定义里配置的全部重试次数；
 * 本轮次数用尽才算真正失败并停掉下游；并发续跑各请求拿到同一次续跑的一致终态结果；
 * 续跑中途重启后剩余重试预算与历史尝试记录都如实保留；续跑里的重试不突破并发上限。
 */
class ResumeRetryTest {

    @TempDir
    Path dataDir;

    /** 前 failTimes 次调用抛异常、之后成功的处理器，calls 记录真实调用次数。 */
    private static StepHandler failTimes(int failTimes, AtomicInteger calls) {
        return (wf, stepId, param) -> {
            int n = calls.incrementAndGet();
            if (n <= failTimes) {
                throw new IllegalStateException("第 " + n + " 次调用故意失败");
            }
        };
    }

    @Test
    void resumeGrantsFreshRetryBudgetAndEventuallySucceeds() {
        AtomicInteger calls = new AtomicInteger();
        // maxRetries=2 → 每轮最多 3 次尝试；前 4 次调用失败：
        // 第一轮 3 次尝试用尽 → FAILED；续跑轮第 1 次（总第 4 次）失败、第 2 次（总第 5 次）成功
        List<StepDef> steps = List.of(
                TestSupport.step("flaky", "flaky", null, 0L, 2, 20L),
                TestSupport.noop("after", "flaky"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flaky", failTimes(4, calls)));
        engine.submit(new WorkflowDef("wf-resume-retry-ok", steps));

        WorkflowExecution failed = TestSupport.awaitTerminal(engine, "wf-resume-retry-ok");
        assertEquals(WorkflowStatus.FAILED, failed.getStatus());
        assertEquals(3, failed.getSteps().get("flaky").getAttempts(), "第一轮 1+2 次尝试用尽才失败");
        assertEquals(StepStatus.SKIPPED, failed.getSteps().get("after").getStatus());

        SubmitOutcome outcome = engine.resume("wf-resume-retry-ok");
        assertFalse(outcome.deduplicated());
        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-retry-ok");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus(),
                "续跑轮重新拿到重试预算，重试到成功整体应为 SUCCESS");
        StepRun flaky = exec.getSteps().get("flaky");
        assertEquals(StepStatus.SUCCESS, flaky.getStatus());
        assertEquals(5, calls.get(), "处理器共被调用 5 次（第一轮 3 次 + 续跑轮 2 次）");
        assertEquals(5, flaky.getAttempts(), "历史尝试次数如实累计，不回退也不重复累加");
        assertEquals(3, flaky.getRoundBaseAttempts(), "续跑把本轮重试基线抬到第一轮结束时的 attempts");
        assertEquals(5, flaky.getAttemptRecords().size(), "两轮的每次尝试都有溯源记录");
        List<AttemptRecord> records = flaky.getAttemptRecords();
        for (int i = 0; i < 4; i++) {
            assertFalse(records.get(i).isSuccess(), "第 " + (i + 1) + " 次尝试应记录为失败");
        }
        assertTrue(records.get(4).isSuccess(), "续跑轮第 2 次尝试成功");
        assertNull(flaky.getFailure(), "最终成功后失败信息应清除");
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("after").getStatus(),
                "续跑轮重试成功后下游正常补跑");
    }

    @Test
    void resumeRetryExhaustedAgainCountsAsRealFailure() {
        // maxRetries=2 → 每轮 3 次尝试；处理器永远失败：每一轮都必须用光 3 次才算失败
        List<StepDef> steps = List.of(
                TestSupport.step("bad", "fail", "boom", 0L, 2, 20L),
                TestSupport.noop("child", "bad"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-resume-retry-exhausted", steps));

        WorkflowExecution first = TestSupport.awaitTerminal(engine, "wf-resume-retry-exhausted");
        assertEquals(WorkflowStatus.FAILED, first.getStatus());
        assertEquals(3, first.getSteps().get("bad").getAttempts());

        engine.resume("wf-resume-retry-exhausted");
        WorkflowExecution second = TestSupport.awaitTerminal(engine, "wf-resume-retry-exhausted");
        assertEquals(WorkflowStatus.FAILED, second.getStatus(),
                "续跑轮重试次数同样用尽后才算真正失败");
        assertEquals(6, second.getSteps().get("bad").getAttempts(),
                "续跑轮完整消耗 1+2 次尝试（3+3=6），不是只跑一次就失败");

        engine.resume("wf-resume-retry-exhausted");
        WorkflowExecution third = TestSupport.awaitTerminal(engine, "wf-resume-retry-exhausted");
        TestSupport.trace(third);
        engine.shutdown();

        assertEquals(WorkflowStatus.FAILED, third.getStatus());
        StepRun bad = third.getSteps().get("bad");
        assertEquals(9, bad.getAttempts(), "每一轮续跑都重新获得完整重试预算（3+3+3=9）");
        assertEquals(9, bad.getAttemptRecords().size(), "三轮的尝试记录全部保留");
        assertTrue(bad.getAttemptRecords().stream().noneMatch(AttemptRecord::isSuccess));
        assertEquals(StepStatus.SKIPPED, third.getSteps().get("child").getStatus(),
                "续跑轮重试耗尽后的失败仍按既有规则停掉下游");
        assertEquals(0, third.getSteps().get("child").getAttempts(), "下游从未被执行");
    }

    @Test
    void concurrentResumeRequestsObserveConsistentFinalResult() throws Exception {
        AtomicInteger bCalls = new AtomicInteger();
        StepHandler flakySlow = (wf, stepId, param) -> {
            if (bCalls.incrementAndGet() == 1) {
                throw new IllegalStateException("首次执行故意失败");
            }
            Thread.sleep(300); // 让续跑轮有足够的 RUNNING 窗口供并发请求命中
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
        assertTrue(ready.await(10, java.util.concurrent.TimeUnit.SECONDS));
        go.countDown();

        List<SubmitOutcome> outcomes = new ArrayList<>();
        for (Future<SubmitOutcome> f : futures) {
            outcomes.add(f.get(30, java.util.concurrent.TimeUnit.SECONDS));
        }
        pool.shutdown();

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-consistent");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        long accepted = outcomes.stream().filter(o -> !o.deduplicated()).count();
        assertEquals(1, accepted, "并发续跑只能有一个真正受理");
        assertEquals(2, bCalls.get(), "步骤 b 总共只执行两次，并发请求没有各跑一遍");

        for (SubmitOutcome outcome : outcomes) {
            if (!outcome.deduplicated()) {
                continue; // 真正受理的请求返回的是受理时刻快照，允许是中间态
            }
            WorkflowExecution seen = outcome.execution();
            assertEquals(WorkflowStatus.SUCCESS, seen.getStatus(),
                    "幂等命中的请求必须等到本轮结束，拿到一致终态而非半截快照");
            assertEquals(2, seen.getSteps().get("b").getAttempts(),
                    "各请求看到的步骤尝试次数必须一致");
            assertEquals(StepStatus.SUCCESS, seen.getSteps().get("c").getStatus());
        }
        long settled = outcomes.stream().filter(SubmitOutcome::deduplicated).count();
        assertTrue(settled > 0, "压测应确实命中并发的幂等续跑路径");
    }

    @Test
    void resumeRetryBudgetAndHistorySurviveRestart() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        // maxRetries=3 → 每轮 4 次尝试；前 5 次调用失败：
        // 第一轮 4 次用尽 → FAILED；续跑轮第 1 次（总第 5 次）失败后"崩溃"，
        // 恢复后剩余预算仍然有效，第 6 次调用成功
        List<StepDef> steps = List.of(
                TestSupport.step("flaky", "flaky", null, 0L, 3, 2000L),
                TestSupport.noop("after", "flaky"));

        WorkflowEngine first = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flaky", failTimes(5, calls)));
        first.submit(new WorkflowDef("wf-resume-retry-restart", steps));
        assertEquals(WorkflowStatus.FAILED,
                TestSupport.awaitTerminal(first, "wf-resume-retry-restart").getStatus());
        assertEquals(4, calls.get());

        first.resume("wf-resume-retry-restart");
        // 等续跑轮第一次尝试失败落盘（attempts=5、步骤回到 PENDING 等重试），随后模拟进程终止
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            StepRun run = first.get("wf-resume-retry-restart").orElseThrow().getSteps().get("flaky");
            if (run.getAttempts() == 5 && run.getStatus() == StepStatus.PENDING) {
                break;
            }
            Thread.sleep(5);
        }
        assertEquals(5, first.get("wf-resume-retry-restart").orElseThrow()
                .getSteps().get("flaky").getAttempts(), "续跑轮第一次尝试应已失败落盘");
        first.shutdown(); // 模拟进程突然终止（retryDelayMs=2000，恢复前不会派发下一次重试）

        WorkflowEngine second = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("flaky", failTimes(5, calls)));
        second.recoverOnStartup();
        WorkflowExecution exec = TestSupport.awaitTerminal(second, "wf-resume-retry-restart");
        TestSupport.trace(exec);
        second.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus(),
                "重启后续跑轮接着跑完，剩余重试预算内成功");
        StepRun flaky = exec.getSteps().get("flaky");
        assertEquals(6, calls.get(), "处理器共调用 6 次：第一轮 4 + 续跑轮重启前 1 + 恢复后 1");
        assertEquals(6, flaky.getAttempts(), "历史尝试次数如实累计，不重复累加也不丢");
        assertEquals(4, flaky.getRoundBaseAttempts(), "续跑轮基线随状态落盘，重启后依然有效");
        assertEquals(6, flaky.getAttemptRecords().size(), "每次尝试的记录都保留");
        List<AttemptRecord> records = flaky.getAttemptRecords();
        for (int i = 0; i < 5; i++) {
            assertFalse(records.get(i).isSuccess());
            assertTrue(records.get(i).getFinishedAt() > 0, "第 " + (i + 1) + " 次尝试记录完整");
        }
        assertTrue(records.get(5).isSuccess());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("after").getStatus());
    }

    @Test
    void resumeRetryRespectsConcurrencyCap() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        Map<String, AtomicInteger> perStepCalls = new java.util.concurrent.ConcurrentHashMap<>();
        // param 为该步骤累计失败次数：x 失败 3 次、y 失败 1 次
        StepHandler busyFlaky = (wf, stepId, param) -> {
            int cur = running.incrementAndGet();
            maxObserved.accumulateAndGet(cur, Math::max);
            try {
                Thread.sleep(100);
                int n = perStepCalls.computeIfAbsent(stepId, k -> new AtomicInteger())
                        .incrementAndGet();
                if (n <= Integer.parseInt(param)) {
                    throw new IllegalStateException("故意失败以触发续跑与重试");
                }
            } finally {
                running.decrementAndGet();
            }
        };
        // 两个步骤各 maxRetries=1（每轮 2 次尝试），并发上限 1。
        // 第一轮：x 两次尝试用尽 → FAILED，y 被级联 SKIPPED（0 次尝试）；
        // 续跑轮：x 第 3 次失败、重试第 4 次成功；y 第 1 次失败、重试第 2 次成功。
        List<StepDef> steps = List.of(
                TestSupport.step("x", "busyFlaky", "3", 0L, 1, 10L),
                TestSupport.step("y", "busyFlaky", "1", 0L, 1, 10L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 1, 5000,
                Map.of("busyFlaky", busyFlaky));
        engine.submit(new WorkflowDef("wf-resume-retry-cap", steps));
        WorkflowExecution firstRound = TestSupport.awaitTerminal(engine, "wf-resume-retry-cap");
        assertEquals(WorkflowStatus.FAILED, firstRound.getStatus());
        assertEquals(2, firstRound.getSteps().get("x").getAttempts(),
                "第一轮 x 的 1+1 次尝试用尽才失败");
        assertEquals(StepStatus.SKIPPED, firstRound.getSteps().get("y").getStatus());

        engine.resume("wf-resume-retry-cap");
        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-resume-retry-cap");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        assertTrue(maxObserved.get() <= 1,
                "续跑轮里的重试同样不得突破并发上限，观测到最大并发 " + maxObserved.get());
        assertEquals(4, exec.getSteps().get("x").getAttempts(),
                "续跑轮重新获得 1+1 次预算（2+2=4）");
        assertEquals(2, exec.getSteps().get("y").getAttempts(),
                "y 在续跑轮补跑并重试一次成功（0+2=2）");
    }
}
