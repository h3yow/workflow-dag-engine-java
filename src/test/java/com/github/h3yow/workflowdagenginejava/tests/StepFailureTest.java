package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
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
 * 失败传播测试：
 * 步骤内部异常 → STEP_EXCEPTION；超时 → TIMEOUT；下游一律 SKIPPED；
 * 成功步骤保持终态；并发数有明确上限；失败时在途独立步骤允许跑完。
 */
class StepFailureTest {

    @TempDir
    Path dataDir;

    @Test
    void stepExceptionPropagatesDownstream() {
        List<StepDef> steps = List.of(
                TestSupport.noop("a"),
                TestSupport.step("b", "fail", "boom", 0L, "a"),
                TestSupport.noop("c", "b"),
                TestSupport.noop("d", "c"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-fail", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-fail");
        TestSupport.trace(exec);

        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("a").getStatus());
        StepRun b = exec.getSteps().get("b");
        assertEquals(StepStatus.FAILED, b.getStatus());
        assertEquals(FailureType.STEP_EXCEPTION, b.getFailure().getFailureType());
        assertTrue(b.getFailure().getMessage().contains("boom"));
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("c").getStatus());
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("d").getStatus());
        assertEquals("b", exec.getFailure().getStepId());
    }

    @Test
    void timeoutIsClassifiedAsTimeout() {
        long start = System.currentTimeMillis();
        List<StepDef> steps = List.of(
                TestSupport.step("slow", "sleep", "1000", 100L),
                TestSupport.noop("after", "slow"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-timeout", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-timeout");
        TestSupport.trace(exec);

        StepRun slow = exec.getSteps().get("slow");
        assertEquals(StepStatus.FAILED, slow.getStatus());
        assertEquals(FailureType.TIMEOUT, slow.getFailure().getFailureType());
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("after").getStatus());
        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        TestSupport.assertDurationBetween(start, 100, 900);
    }

    @Test
    void parallelStepsRespectConcurrencyCap() throws Exception {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maxObserved = new AtomicInteger();
        StepHandler busy = (wf, stepId, param) -> {
            int cur = running.incrementAndGet();
            maxObserved.accumulateAndGet(cur, Math::max);
            Thread.sleep(200);
            running.decrementAndGet();
        };
        List<StepDef> steps = List.of(
                TestSupport.step("p1", "busy", null, 0L),
                TestSupport.step("p2", "busy", null, 0L),
                TestSupport.step("p3", "busy", null, 0L),
                TestSupport.step("p4", "busy", null, 0L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of("busy", busy));
        long start = System.currentTimeMillis();
        engine.submit(new WorkflowDef("wf-parallel", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-parallel");
        TestSupport.trace(exec);
        engine.shutdown();

        assertEquals(WorkflowStatus.SUCCESS, exec.getStatus());
        assertTrue(maxObserved.get() <= 2, "观测到的最大并发 " + maxObserved.get() + " 超过上限 2");
        assertEquals(2, maxObserved.get(), "无依赖步骤应当并行执行");
        // 4 个 200ms 步骤、并发 2，应约 400ms 完成；串行需要 800ms
        TestSupport.assertDurationBetween(start, 350, 700);
    }

    @Test
    void unsupportedStepTypeIsDefinitionError() {
        List<StepDef> steps = List.of(
                TestSupport.step("a", "unknown-type", null, 0L));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 4, 5000, Map.of());

        com.github.h3yow.workflowdagenginejava.validation.InvalidWorkflowException ex =
                assertThrows(com.github.h3yow.workflowdagenginejava.validation.InvalidWorkflowException.class,
                        () -> engine.submit(new WorkflowDef("wf-def-error", steps)));
        org.slf4j.LoggerFactory.getLogger("workflow-test").info(
                "[workflowId=wf-def-error] 最终状态=REJECTED/{} 原因={}",
                ex.getReason(), ex.getMessage());
        assertEquals(com.github.h3yow.workflowdagenginejava.model.RejectReason.INVALID_STEP,
                ex.getReason());
        assertTrue(engine.get("wf-def-error").isEmpty(),
                "定义错误不得产生任何执行记录");
    }

    @Test
    void inFlightIndependentStepCanFinishButDownstreamOfFailureSkipped() {
        List<StepDef> steps = List.of(
                TestSupport.step("failsFast", "fail", null, 0L),
                TestSupport.step("slow", "sleep", "300", 0L),
                TestSupport.noop("child", "failsFast"));
        WorkflowEngine engine = TestSupport.newEngine(dataDir, 2, 5000, Map.of());
        engine.submit(new WorkflowDef("wf-inflight", steps));

        WorkflowExecution exec = TestSupport.awaitTerminal(engine, "wf-inflight");
        TestSupport.trace(exec);

        assertEquals(WorkflowStatus.FAILED, exec.getStatus());
        assertEquals(StepStatus.SUCCESS, exec.getSteps().get("slow").getStatus(),
                "失败发生时已在执行的独立步骤允许跑完，且不得重跑");
        assertEquals(StepStatus.SKIPPED, exec.getSteps().get("child").getStatus());
        assertEquals(1, exec.getSteps().get("slow").getAttempts());
    }
}
