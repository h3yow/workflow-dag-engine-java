package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.engine.StepHandler;
import com.github.h3yow.workflowdagenginejava.engine.StepHandlerRegistry;
import com.github.h3yow.workflowdagenginejava.engine.WorkflowEngine;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.StepRun;
import com.github.h3yow.workflowdagenginejava.model.WorkflowExecution;
import com.github.h3yow.workflowdagenginejava.model.WorkflowStatus;
import com.github.h3yow.workflowdagenginejava.store.FileWorkflowStateStore;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 测试公共工具：临时目录构建引擎、等待终态、打印工作流标识/依赖关系/最终状态。
 */
final class TestSupport {

    private TestSupport() {
    }

    static StepDef step(String id, String type, String param, long timeoutMs, String... deps) {
        return new StepDef(id, type, List.of(deps), param, timeoutMs);
    }

    static StepDef noop(String id, String... deps) {
        return step(id, "noop", null, 0L, deps);
    }

    /** 不经过 Spring、不触发 @PostConstruct，测试自行决定何时恢复。 */
    static WorkflowEngine newEngine(Path dataDir, int concurrency, long defaultTimeoutMs,
                                    Map<String, StepHandler> extra) {
        Map<String, StepHandler> handlers = new LinkedHashMap<>(
                com.github.h3yow.workflowdagenginejava.engine.BuiltInHandlers.defaults());
        handlers.putAll(extra);
        return new WorkflowEngine(new FileWorkflowStateStore(dataDir.toString()),
                new StepHandlerRegistry(handlers), concurrency, defaultTimeoutMs);
    }

    static WorkflowExecution awaitTerminal(WorkflowEngine engine, String workflowId) {
        long deadline = System.currentTimeMillis() + 30_000;
        WorkflowExecution exec = null;
        while (System.currentTimeMillis() < deadline) {
            exec = engine.get(workflowId).orElseThrow();
            if (exec.getStatus() != WorkflowStatus.RUNNING) {
                return exec;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new AssertionError("工作流 " + workflowId + " 未在超时内结束: "
                + (exec == null ? null : exec.getStatus()));
    }

    /** 打印工作流标识、步骤依赖关系与每个步骤最终状态（满足测试日志溯源要求）。 */
    static void trace(WorkflowExecution exec) {
        Map<String, List<String>> deps = new LinkedHashMap<>();
        Map<String, String> finalStatus = new LinkedHashMap<>();
        for (StepDef s : exec.getDefinition().steps()) {
            deps.put(s.id(), s.dependsOn());
            StepRun run = exec.getSteps().get(s.id());
            String extra = run.getFailure() == null
                    ? run.getStatus().name()
                    : run.getStatus() + "/" + run.getFailure().getFailureType();
            finalStatus.put(s.id(), extra);
        }
        org.slf4j.LoggerFactory.getLogger("workflow-test").info(
                "[workflowId={}] 步骤依赖关系={} 工作流最终状态={} 各步骤最终状态={} 根失败={}",
                exec.getWorkflowId(), deps, exec.getStatus(), finalStatus, exec.getFailure());
    }

    static void assertDurationBetween(long start, long minMs, long maxMs) {
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed >= minMs, "耗时 " + elapsed + "ms 短于预期下限 " + minMs + "ms");
        assertTrue(elapsed <= maxMs, "耗时 " + elapsed + "ms 超过预期上限 " + maxMs + "ms");
    }
}
