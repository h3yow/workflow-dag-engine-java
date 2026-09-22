package com.github.h3yow.workflowdagenginejava.validation;

import com.github.h3yow.workflowdagenginejava.model.RejectReason;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * DAG 依赖校验：拒绝缺失依赖、自引用、成环、非法标识/空定义。
 */
public final class DagValidator {

    private DagValidator() {
    }

    /**
     * 校验工作流定义。
     *
     * @throws InvalidWorkflowException 校验失败，可通过 getReason() 区分拒绝原因
     */
    public static void validate(WorkflowDef def) {
        if (def == null) {
            throw new InvalidWorkflowException(RejectReason.INVALID_ID, "工作流定义为空");
        }
        if (def.workflowId() == null || def.workflowId().isBlank()) {
            throw new InvalidWorkflowException(RejectReason.INVALID_ID, "workflowId 不能为空");
        }
        if (def.steps() == null || def.steps().isEmpty()) {
            throw new InvalidWorkflowException(RejectReason.EMPTY_WORKFLOW,
                    "工作流 " + def.workflowId() + " 不包含任何步骤");
        }

        Map<String, StepDef> byId = new LinkedHashMap<>();
        for (StepDef step : def.steps()) {
            if (step == null || step.id() == null || step.id().isBlank()) {
                throw new InvalidWorkflowException(RejectReason.INVALID_ID,
                        "工作流 " + def.workflowId() + " 中存在没有 id 的步骤");
            }
            if (step.type() == null || step.type().isBlank()) {
                throw new InvalidWorkflowException(RejectReason.INVALID_STEP,
                        "步骤 " + step.id() + " 缺少 type");
            }
            if (byId.put(step.id(), step) != null) {
                throw new InvalidWorkflowException(RejectReason.INVALID_ID,
                        "步骤 id 重复: " + step.id());
            }
        }

        // 依赖引用校验：自引用与缺失依赖（在环检测之前，保证原因可区分）
        for (StepDef step : def.steps()) {
            for (String dep : step.dependsOn()) {
                if (dep == null || dep.isBlank()) {
                    throw new InvalidWorkflowException(RejectReason.MISSING_DEPENDENCY,
                            "步骤 " + step.id() + " 声明了空的依赖标识");
                }
                if (dep.equals(step.id())) {
                    throw new InvalidWorkflowException(RejectReason.SELF_REFERENCE,
                            "步骤 " + step.id() + " 依赖自身");
                }
                if (!byId.containsKey(dep)) {
                    throw new InvalidWorkflowException(RejectReason.MISSING_DEPENDENCY,
                            "步骤 " + step.id() + " 依赖了不存在的步骤: " + dep);
                }
            }
        }

        // 环检测：Kahn 拓扑排序无法消费全部节点即说明存在环
        List<String> order = topologicalOrder(def);
        if (order.size() != byId.size()) {
            throw new InvalidWorkflowException(RejectReason.CYCLE_DETECTED,
                    "工作流 " + def.workflowId() + " 的依赖关系中存在环");
        }
    }

    /**
     * 基于 Kahn 算法产出满足依赖约束的拓扑顺序；同层候选按步骤 id 字典序排序，
     * 保证执行顺序仅依赖 DAG 结构，与提交顺序无关、多次运行可复现。
     */
    public static List<String> topologicalOrder(WorkflowDef def) {
        Map<String, StepDef> byId = indexById(def);
        Map<String, Integer> indegree = new LinkedHashMap<>();
        Map<String, List<String>> adjacency = new LinkedHashMap<>();
        for (String id : byId.keySet()) {
            indegree.put(id, 0);
            adjacency.put(id, new ArrayList<>());
        }
        for (StepDef step : def.steps()) {
            for (String dep : step.dependsOn()) {
                if (byId.containsKey(dep)) {
                    adjacency.get(dep).add(step.id());
                    indegree.merge(step.id(), 1, Integer::sum);
                }
            }
        }

        TreeSet<String> ready = new TreeSet<>();
        for (Map.Entry<String, Integer> e : indegree.entrySet()) {
            if (e.getValue() == 0) {
                ready.add(e.getKey());
            }
        }
        List<String> order = new ArrayList<>();
        while (!ready.isEmpty()) {
            String id = ready.pollFirst();
            order.add(id);
            for (String next : adjacency.get(id)) {
                int d = indegree.merge(next, -1, Integer::sum);
                if (d == 0) {
                    ready.add(next);
                }
            }
        }
        return order;
    }

    /** 按 id 索引步骤，保持首次出现顺序。 */
    public static Map<String, StepDef> indexById(WorkflowDef def) {
        Map<String, StepDef> byId = new LinkedHashMap<>();
        if (def == null || def.steps() == null) {
            return byId;
        }
        for (StepDef step : def.steps()) {
            if (step != null && step.id() != null) {
                byId.putIfAbsent(step.id(), step);
            }
        }
        return byId;
    }
}
