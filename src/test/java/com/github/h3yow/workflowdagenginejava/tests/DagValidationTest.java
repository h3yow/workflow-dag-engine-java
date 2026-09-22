package com.github.h3yow.workflowdagenginejava.tests;

import com.github.h3yow.workflowdagenginejava.model.RejectReason;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import com.github.h3yow.workflowdagenginejava.validation.DagValidator;
import com.github.h3yow.workflowdagenginejava.validation.InvalidWorkflowException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 依赖校验测试：缺失依赖、自引用、成环必须被拒绝且原因可区分；
 * 合法 DAG 的执行顺序满足依赖约束且与提交顺序无关。
 */
class DagValidationTest {

    private static final Logger log = LoggerFactory.getLogger(DagValidationTest.class);

    /** 打印工作流标识、步骤依赖关系与最终结论，便于测试日志溯源。 */
    private static void trace(String workflowId, List<StepDef> steps, String conclusion) {
        Map<String, List<String>> deps = new LinkedHashMap<>();
        for (StepDef s : steps) {
            deps.put(s.id(), s.dependsOn());
        }
        log.info("[workflowId={}] 步骤依赖关系={} 最终状态={}", workflowId, deps, conclusion);
    }

    private static StepDef step(String id, String... deps) {
        return new StepDef(id, "noop", List.of(deps));
    }

    @Test
    void rejectsMissingDependency() {
        List<StepDef> steps = List.of(step("a", "ghost"));
        WorkflowDef def = new WorkflowDef("wf-missing-dep", steps);

        InvalidWorkflowException ex = assertThrows(InvalidWorkflowException.class,
                () -> DagValidator.validate(def));
        trace(def.workflowId(), steps, "REJECTED/" + ex.getReason());

        assertEquals(RejectReason.MISSING_DEPENDENCY, ex.getReason());
    }

    @Test
    void rejectsSelfReference() {
        List<StepDef> steps = List.of(step("a", "a"));
        WorkflowDef def = new WorkflowDef("wf-self-ref", steps);

        InvalidWorkflowException ex = assertThrows(InvalidWorkflowException.class,
                () -> DagValidator.validate(def));
        trace(def.workflowId(), steps, "REJECTED/" + ex.getReason());

        assertEquals(RejectReason.SELF_REFERENCE, ex.getReason());
    }

    @Test
    void rejectsCycle() {
        List<StepDef> steps = List.of(
                step("a", "c"),
                step("b", "a"),
                step("c", "b"));
        WorkflowDef def = new WorkflowDef("wf-cycle", steps);

        InvalidWorkflowException ex = assertThrows(InvalidWorkflowException.class,
                () -> DagValidator.validate(def));
        trace(def.workflowId(), steps, "REJECTED/" + ex.getReason());

        assertEquals(RejectReason.CYCLE_DETECTED, ex.getReason());
    }

    @Test
    void rejectsEmptyWorkflow() {
        WorkflowDef def = new WorkflowDef("wf-empty", List.of());
        InvalidWorkflowException ex = assertThrows(InvalidWorkflowException.class,
                () -> DagValidator.validate(def));
        log.info("[workflowId={}] 步骤依赖关系={} 最终状态={}", def.workflowId(),
                Map.of(), "REJECTED/" + ex.getReason());
        assertEquals(RejectReason.EMPTY_WORKFLOW, ex.getReason());
    }

    @Test
    void topologicalOrderAlwaysSatisfiesDependencies() {
        List<StepDef> steps = List.of(
                step("a"),
                step("b", "a"),
                step("c", "a"),
                step("d", "b", "c"));
        WorkflowDef def = new WorkflowDef("wf-valid", steps);

        assertDoesNotThrow(() -> DagValidator.validate(def));
        List<String> order = DagValidator.topologicalOrder(def);

        // 校验每个步骤都排在其依赖之后
        Map<String, Integer> position = new LinkedHashMap<>();
        for (int i = 0; i < order.size(); i++) {
            position.put(order.get(i), i);
        }
        for (StepDef s : steps) {
            for (String dep : s.dependsOn()) {
                assertTrue(position.get(dep) < position.get(s.id()),
                        dep + " 必须先于 " + s.id());
            }
        }
        trace(def.workflowId(), steps, "ACCEPTED 拓扑序=" + order);
        assertEquals(List.of("a", "b", "c", "d"), order);
    }

    @Test
    void orderIndependentOfSubmissionOrder() {
        List<StepDef> stepsA = List.of(
                step("a"),
                step("c", "a"),
                step("b", "a"),
                step("d", "b", "c"));
        // 同一 DAG，仅步骤提交顺序不同
        List<StepDef> stepsB = new ArrayList<>(List.of(
                step("d", "b", "c"),
                step("b", "a"),
                step("c", "a"),
                step("a")));

        List<String> orderA = DagValidator.topologicalOrder(new WorkflowDef("wf-shuffle-a", stepsA));
        List<String> orderB = DagValidator.topologicalOrder(new WorkflowDef("wf-shuffle-b", stepsB));

        trace("wf-shuffle-a", stepsA, "拓扑序=" + orderA);
        trace("wf-shuffle-b", stepsB, "拓扑序=" + orderB);
        assertEquals(orderA, orderB, "拓扑序必须与提交顺序无关");
    }
}
