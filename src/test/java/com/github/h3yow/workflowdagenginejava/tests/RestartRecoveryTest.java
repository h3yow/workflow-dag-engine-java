package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.DuplicateWorkflowException;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 断点恢复与幂等提交测试：
 * 重复提交返回既有执行、步骤只执行一次；
 * "崩溃"（立即关闭线程池）后用新引擎实例恢复，SUCCESS 步骤不重跑、整体可继续到终态。
 */
class RestartRecoveryTest {

    @TempDir
    Path dataDir;

    @Test
    void duplicateSubmissionReturnsExistingExecution() {
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("e1", "echo", "hello", 0L, "a"),
                TestSupport.noop("b", "a"));
        WorkflowDef def = new WorkflowDef("wf-idem", steps);

        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000, Map.of());
        var first = engine.submit(def);
        assertFalse(first.deduplicated());
        WorkflowExecution finished = TestSupport.awaitTerminal(engine, "wf-idem");

        // 步骤数组顺序不同但 DAG 语义相同，仍视为重复提交
        WorkflowDef reordered = new WorkflowDef("wf-idem", List.of(
                TestSupport.noop("b", "a"),
                TestSupport.step("e1", "echo", "hello", 0L, "a"),
                TestSupport.noop("a")));
        var second = engine.submit(reordered);
        assertTrue(second.deduplicated(), "重复提交必须命中幂等，不产生第二次执行");
        assertSame(finished.getStatus(), second.execution().getStatus());
        assertEquals(WorkflowStatus.SUCCESS, second.execution().getStatus());
        TestSupport.trace(second.execution());
    }

    @Test
    void sameIdDifferentDefinitionRejectedAsConflict() {
        WorkflowDef v1 = new WorkflowDef("wf-conflict", List.of(TestSupport.noop("a")));
        WorkflowDef v2 = new WorkflowDef("wf-conflict",
                List.of(TestSupport.noop("a"), TestSupport.noop("b")));

        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000, Map.of());
        engine.submit(v1);
        TestSupport.awaitTerminal(engine, "wf-conflict");

        DuplicateWorkflowException ex = assertThrows(DuplicateWorkflowException.class,
                () -> engine.submit(v2));
        org.slf4j.LoggerFactory.getLogger("workflow-test")
                .info("[workflowId=wf-conflict] 重复提交不同定义 → 拒绝: {}", ex.getMessage());
    }

    @Test
    void restartResumesWithoutRerunningSucceededSteps() throws Exception {
        AtomicInteger executionsOfB = new AtomicInteger();
        StepHandler countB = (wf, stepId, param) -> executionsOfB.incrementAndGet();

        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "countB", null, 0L, "a"),
                TestSupport.step("c", "sleep", "300", 0L, "b"),
                TestSupport.noop("d", "c"));

        // 第一次"进程"：让 a、b 完成，c 在 RUNNING 时崩溃
        WorkflowEngine first = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("countB", countB));
        first.submit(new WorkflowDef("wf-restart", steps));
        waitUntilStep(first, "wf-restart", "c", StepStatus.RUNNING);
        first.shutdown(); // 模拟进程突然终止（shutdownNow 中断在途步骤）

        // 第二次"进程"：从同一目录启动并恢复
        WorkflowEngine second = TestSupport.newEngine(dataDir, 2, 5000,
                Map.of("countB", countB));
        second.recoverOnStartup();

        WorkflowExecution resumed = TestSupport.awaitTerminal(second, "wf-restart");
        TestSupport.trace(resumed);

        assertEquals(WorkflowStatus.SUCCESS, resumed.getStatus());
        assertEquals(StepStatus.SUCCESS, resumed.getSteps().get("a").getStatus());
        assertEquals(StepStatus.SUCCESS, resumed.getSteps().get("d").getStatus());
        assertEquals(1, executionsOfB.get(),
                "重启前已 SUCCESS 的步骤 b 不得在恢复后重跑");
        assertEquals(1, resumed.getSteps().get("b").getAttempts(),
                "溯源记录 attempts 也必须保持一致");
        second.shutdown();
    }

    @Test
    void persistedProgressSurvivesEngineRestart() {
        List<StepDef> steps = List.of(TestSupport.noop("x"));
        WorkflowEngine first = TestSupport.newEngine(dataDir, 1, 5000, Map.of());
        first.submit(new WorkflowDef("wf-persist", steps));
        TestSupport.awaitTerminal(engine(first), "wf-persist");
        first.shutdown();

        // 全新引擎仅加载状态，不调度任何东西：终态工作流不会被重跑
        WorkflowEngine second = TestSupport.newEngine(dataDir, 1, 5000, Map.of());
        second.recoverOnStartup();
        WorkflowExecution loaded = second.get("wf-persist").orElseThrow();
        TestSupport.trace(loaded);

        assertEquals(WorkflowStatus.SUCCESS, loaded.getStatus());
        assertEquals(StepStatus.SUCCESS, loaded.getSteps().get("x").getStatus());
    }

    private static WorkflowEngine engine(WorkflowEngine e) {
        return e;
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
}
