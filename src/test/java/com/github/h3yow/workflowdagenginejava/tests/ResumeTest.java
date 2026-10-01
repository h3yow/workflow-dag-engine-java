package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败续跑测试：
 * 只补跑未成功的步骤、成功步骤不重跑且尝试历史保留；
 * 并发重复续跑只真正执行一次，所有请求拿到同一份终态；
 * 续跑跑到一半重启后继续，不重复执行、不重复累加尝试次数。
 */
class ResumeTest {

    @TempDir
    Path dataDir;

    @Test
    void resumeRerunsOnlyUnsuccessfulStepsAndKeepsHistory() {
        AtomicInteger aCalls = new AtomicInteger();
        AtomicInteger bCalls = new AtomicInteger();
        StepHandler countA = (wf, id, p) -> aCalls.incrementAndGet();
        StepHandler flakyB = (wf, id, p) -> {
            if (bCalls.incrementAndGet() == 1) {
                throw new IllegalStateException("first-run-bug");
            }
        };

        List<StepDef> def = List.of(
                TestSupport.step("a", "countA", null, 0L),
                TestSupport.step("b", "flakyB", null, 0L, "a"),
                TestSupport.noop("c", "b"),
                TestSupport.noop("d", "b"));

        // 第一批：b 失败，c/d 被跳过
        WorkflowEngine first = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("countA", countA, "flakyB", flakyB));
        first.submit(new WorkflowDef("wf-resume", def));
        WorkflowExecution failed = TestSupport.awaitTerminal(first, "wf-resume");
        TestSupport.trace(failed);
        assertEquals(WorkflowStatus.FAILED, failed.getStatus());
        assertEquals(StepStatus.SUCCESS, failed.getSteps().get("a").getStatus());
        assertEquals(StepStatus.FAILED, failed.getSteps().get("b").getStatus());
        assertEquals(StepStatus.SKIPPED, failed.getSteps().get("c").getStatus());
        assertEquals(StepStatus.SKIPPED, failed.getSteps().get("d").getStatus());
        StepRun bBefore = failed.getSteps().get("b");
        int bAttemptsBefore = bBefore.getAttempts();
        assertEquals(1, bAttemptsBefore);

        // 续跑
        var resumeOutcome = first.resume("wf-resume");
        assertFalse(resumeOutcome.deduplicated(), "首次续跑是真正的执行");
        WorkflowExecution done = TestSupport.awaitTerminal(first, "wf-resume");
        TestSupport.trace(done);

        assertEquals(WorkflowStatus.SUCCESS, done.getStatus());
        assertEquals(1, done.getResumeCount());
        assertEquals(1, aCalls.get(), "已成功的 a 绝不重跑");
        assertEquals(StepStatus.SUCCESS, done.getSteps().get("a").getStatus());
        assertEquals(2, bCalls.get(), "b 在续跑批次再执行一次");
        StepRun bAfter = done.getSteps().get("b");
        assertEquals(StepStatus.SUCCESS, bAfter.getStatus());
        // 尝试历史跨批次保留、连续编号、不重复累加
        assertEquals(2, bAfter.getAttempts());
        assertEquals(2, bAfter.getAttemptRecords().size());
        assertEquals(1, bAfter.getAttemptRecords().get(0).attemptNo());
        assertEquals(1, bAfter.getAttemptRecords().get(0).runNo());
        assertEquals(2, bAfter.getAttemptRecords().get(1).attemptNo());
        assertEquals(2, bAfter.getAttemptRecords().get(1).runNo());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.AttemptResult.STEP_EXCEPTION,
                bAfter.getAttemptRecords().get(0).result());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.AttemptResult.SUCCESS,
                bAfter.getAttemptRecords().get(1).result());
        // 之前因上游失败被跳过的步骤按依赖补跑
        assertEquals(StepStatus.SUCCESS, done.getSteps().get("c").getStatus());
        assertEquals(StepStatus.SUCCESS, done.getSteps().get("d").getStatus());
        // 已结束（成功）的工作流再续跑/再提交，行为与重复提交一致，不产生第二次执行
        var again = first.resume("wf-resume");
        assertTrue(again.deduplicated());
        assertEquals(WorkflowStatus.SUCCESS, again.execution().getStatus());
        var resubmit = first.submit(new WorkflowDef("wf-resume", def));
        assertTrue(resubmit.deduplicated());
        assertEquals(2, bCalls.get());
        first.shutdown();
    }

    @Test
    void concurrentResumesExecuteOnlyOnceAndAllGetSameResult() throws Exception {
        AtomicInteger bCalls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        List<StepDef> def = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "switchable", null, 0L, "a"),
                TestSupport.noop("c", "b"));

        // 第一批：b 快速失败；续跑前替换成"进入后阻塞在门闩上"的处理器以制造并发窗口
        SwitchableHandler switchable = new SwitchableHandler();
        switchable.setDelegate((wf, id, p) -> {
            throw new IllegalStateException("bug");
        });
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("switchable", switchable));
        engine.submit(new WorkflowDef("wf-cresume", def));
        WorkflowExecution failed = TestSupport.awaitTerminal(engine, "wf-cresume");
        assertEquals(WorkflowStatus.FAILED, failed.getStatus());

        switchable.setDelegate((wf, id, p) -> {
            bCalls.incrementAndGet();
            entered.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS), "测试门闩未被释放");
        });

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentLinkedQueue<WorkflowExecution> results = new ConcurrentLinkedQueue<>();
        CountDownLatch finished = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    results.add(engine.resume("wf-cresume").execution());
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    finished.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(entered.await(10, TimeUnit.SECONDS), "续跑步骤未开始");
        // b 仍在执行期间：续跑确实已开始，并发请求应正排队等待同一份结果
        Thread.sleep(50);
        release.countDown();
        assertTrue(finished.await(20, TimeUnit.SECONDS));
        pool.shutdown();

        WorkflowExecution terminal = TestSupport.awaitTerminal(engine, "wf-cresume");
        TestSupport.trace(terminal);
        assertEquals(WorkflowStatus.SUCCESS, terminal.getStatus());
        assertEquals(1, bCalls.get(), "并发续跑只能真正执行一次 b");
        assertEquals(threads, results.size());
        for (WorkflowExecution r : results) {
            assertEquals(WorkflowStatus.SUCCESS, r.getStatus(),
                    "所有并发续跑请求必须拿到同一份终态结果");
        }
        assertEquals(1, terminal.getResumeCount());
        engine.shutdown();
    }

    @Test
    void resumeMidwayRestartContinuesWithoutRerunOrDoubleCounting() throws Exception {
        AtomicInteger bCalls = new AtomicInteger();
        AtomicInteger cCalls = new AtomicInteger();
        StepHandler countB = (wf, id, p) -> bCalls.incrementAndGet();
        StepHandler slowC = (wf, id, p) -> {
            cCalls.incrementAndGet();
            Thread.sleep(400);
        };

        List<StepDef> def = List.of(
                TestSupport.step("b", "countB", null, 0L, "a"),
                TestSupport.step("c", "slowC", null, 0L, "b"),
                TestSupport.noop("d", "c"),
                TestSupport.noop("a"));

        // 第一批失败：c 失败（用快速失败处理器），d 跳过
        StepHandler fastFailC = (wf, id, p) -> {
            throw new IllegalStateException("bug-in-c");
        };
        WorkflowEngine first = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("countB", countB, "slowC", fastFailC));
        first.submit(new WorkflowDef("wf-rs-restart", def));
        WorkflowExecution failed = TestSupport.awaitTerminal(first, "wf-rs-restart");
        assertEquals(WorkflowStatus.FAILED, failed.getStatus());
        first.shutdown();

        // 第二批：续跑，c 这次变慢；在 c RUNNING 时"崩溃"
        WorkflowEngine second = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("countB", countB, "slowC", slowC));
        second.recoverOnStartup();
        // resume 会阻塞到终态，因此放到独立线程发起，主线程负责在执行途中"杀进程"
        Thread resumeCaller = new Thread(() -> second.resume("wf-rs-restart"), "resume-caller");
        resumeCaller.setDaemon(true);
        resumeCaller.start();
        waitUntilStep(second, "wf-rs-restart", "c", StepStatus.RUNNING);
        second.crash();

        // 第三批：新进程从同一目录恢复，续跑继续跑完
        WorkflowEngine third = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("countB", countB, "slowC", slowC));
        third.recoverOnStartup();
        WorkflowExecution done = TestSupport.awaitTerminal(third, "wf-rs-restart");
        TestSupport.trace(done);

        assertEquals(WorkflowStatus.SUCCESS, done.getStatus());
        assertEquals(1, done.getResumeCount(), "崩溃没有产生额外的续跑批次");
        assertEquals(1, bCalls.get(), "第一批已成功的 b 续跑与重启后都不能重跑");
        // c：第一批失败 1 次 + 续跑被中断 1 次 + 恢复后补跑 1 次 = 3 次尝试、3 条明细
        assertEquals(3, done.getSteps().get("c").getAttempts(),
                "已记录的尝试次数不能被重复累加");
        assertEquals(3, done.getSteps().get("c").getAttemptRecords().size());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.AttemptResult.STEP_EXCEPTION,
                done.getSteps().get("c").getAttemptRecords().get(0).result());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.AttemptResult.INTERRUPTED,
                done.getSteps().get("c").getAttemptRecords().get(1).result());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.AttemptResult.SUCCESS,
                done.getSteps().get("c").getAttemptRecords().get(2).result());
        assertEquals(StepStatus.SUCCESS, done.getSteps().get("d").getStatus());
        assertEquals(StepStatus.SUCCESS, done.getSteps().get("a").getStatus());
        third.shutdown();
    }

    @Test
    void resumeUnknownWorkflowAndRepeatedFailureAreHandled() {
        WorkflowEngine empty = TestSupport.newEngine(dataDir, 4, 5000, Map.of());
        assertThrows(com.github.h3yow.workflowdagenginejava.engine.WorkflowNotFoundException.class,
                () -> empty.resume("no-such-wf"));

        AtomicInteger calls = new AtomicInteger();
        StepHandler alwaysFail = (wf, id, p) -> {
            calls.incrementAndGet();
            throw new IllegalStateException("still-broken");
        };
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000,
                Map.of("alwaysFail", alwaysFail));
        engine.submit(new WorkflowDef("wf-rs-again",
                List.of(TestSupport.step("x", "alwaysFail", null, 0L))));
        TestSupport.awaitTerminal(engine, "wf-rs-again");

        var r1 = engine.resume("wf-rs-again");
        assertFalse(r1.deduplicated());
        WorkflowExecution f2 = TestSupport.awaitTerminal(engine, "wf-rs-again");
        assertEquals(WorkflowStatus.FAILED, f2.getStatus());
        assertEquals(1, f2.getResumeCount());
        assertEquals(2, f2.getSteps().get("x").getAttempts());

        // 修复前允许继续第二次续跑，又是一次真实执行，历史继续累计
        var r2 = engine.resume("wf-rs-again");
        assertFalse(r2.deduplicated());
        WorkflowExecution f3 = TestSupport.awaitTerminal(engine, "wf-rs-again");
        assertEquals(WorkflowStatus.FAILED, f3.getStatus());
        assertEquals(2, f3.getResumeCount());
        assertEquals(3, f3.getSteps().get("x").getAttempts());
        engine.shutdown();
    }

    private static void waitUntilStep(WorkflowEngine engine, String workflowId,
                                      String stepId, StepStatus expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            WorkflowExecution exec = engine.get(workflowId).orElse(null);
            if (exec != null && exec.getSteps().get(stepId).getStatus() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("步骤 " + stepId + " 未进入 " + expected);
    }

    /** 可在运行时替换委托的处理器，用于模拟"修复后再续跑"。 */
    private static final class SwitchableHandler implements StepHandler {
        private volatile StepHandler delegate = (wf, id, p) -> {
        };

        void setDelegate(StepHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(String workflowId, String stepId, String param) throws Exception {
            delegate.execute(workflowId, stepId, param);
        }
    }
}
