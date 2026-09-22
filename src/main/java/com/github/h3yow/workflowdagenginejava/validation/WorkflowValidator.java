package com.github.h3yow.workflowdagenginejava.validation;

import com.github.h3yow.workflowdagenginejava.model.RejectReason;
import com.github.h3yow.workflowdagenginejava.model.StepDef;
import com.github.h3yow.workflowdagenginejava.model.WorkflowDef;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates workflow definitions.
 *
 * <p>Checks are performed in a fixed, deterministic order so the rejection
 * reason is reproducible regardless of submission ordering:
 * <ol>
 *   <li>{@link RejectReason#EMPTY_DEFINITION} - no definition / no steps</li>
 *   <li>{@link RejectReason#EMPTY_WORKFLOW_ID} - blank workflow id</li>
 *   <li>{@link RejectReason#EMPTY_STEP_ID} - a step has a blank id</li>
 *   <li>{@link RejectReason#DUPLICATE_STEP_ID} - two steps share an id</li>
 *   <li>{@link RejectReason#SELF_DEPENDENCY} - a step depends on itself</li>
 *   <li>{@link RejectReason#MISSING_DEPENDENCY} - dependsOn names an unknown step</li>
 *   <li>{@link RejectReason#CYCLIC_DEPENDENCY} - the directed graph has a cycle</li>
 * </ol>
 * A canonicalized (deduplicated, order-normalized) copy of the definition is
 * returned so that execution order depends only on the DAG, never on the
 * order in which steps were submitted.
 */
@Component
public class WorkflowValidator {

    /** Result of a successful validation: the definition in canonical form. */
    public record ValidatedWorkflow(WorkflowDef def, Map<String, StepDef> byId,
                                    Map<String, List<String>> dependencies) {
    }

    public ValidatedWorkflow validate(WorkflowDef def) {
        if (def == null || def.steps() == null || def.steps().isEmpty()) {
            throw new WorkflowValidationException(RejectReason.EMPTY_DEFINITION,
                    "workflow definition must contain at least one step");
        }
        if (def.workflowId() == null || def.workflowId().isBlank()) {
            throw new WorkflowValidationException(RejectReason.EMPTY_WORKFLOW_ID,
                    "workflowId must not be blank");
        }

        // Preserve first-seen order; reject duplicates.
        Map<String, StepDef> byId = new LinkedHashMap<>();
        for (StepDef step : def.steps()) {
            if (step == null || step.id() == null || step.id().isBlank()) {
                throw new WorkflowValidationException(RejectReason.EMPTY_STEP_ID,
                        "every step must declare a non-blank id");
            }
            if (byId.putIfAbsent(step.id(), step) != null) {
                throw new WorkflowValidationException(RejectReason.DUPLICATE_STEP_ID,
                        "duplicate step id: " + step.id());
            }
        }

        // Dependency edges with dedup + self / missing checks.
        Map<String, List<String>> deps = new LinkedHashMap<>();
        for (StepDef step : def.steps()) {
            List<String> declared = step.dependsOn() == null ? List.of() : step.dependsOn();
            Set<String> unique = new HashSet<>();
            List<String> normalized = new ArrayList<>();
            for (String dep : declared) {
                if (step.id().equals(dep)) {
                    throw new WorkflowValidationException(RejectReason.SELF_DEPENDENCY,
                            "step '" + step.id() + "' must not depend on itself");
                }
                if (!byId.containsKey(dep)) {
                    throw new WorkflowValidationException(RejectReason.MISSING_DEPENDENCY,
                            "step '" + step.id() + "' depends on missing step '" + dep + "'");
                }
                if (unique.add(dep)) {
                    normalized.add(dep);
                }
            }
            deps.put(step.id(), normalized);
        }

        detectCycle(byId, deps);

        List<StepDef> canonicalSteps = canonicalize(byId, deps);
        WorkflowDef canonical = new WorkflowDef(def.workflowId(), canonicalSteps,
                Math.max(1, def.maxConcurrency()));
        return new ValidatedWorkflow(canonical, byId, deps);
    }

    /**
     * Kahn's algorithm. If topological processing cannot consume every node,
     * the remaining nodes participate in (or feed into) a cycle.
     */
    private void detectCycle(Map<String, StepDef> byId, Map<String, List<String>> deps) {
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> downstream = new HashMap<>();
        for (String id : byId.keySet()) {
            indegree.put(id, 0);
            downstream.put(id, new ArrayList<>());
        }
        for (Map.Entry<String, List<String>> e : deps.entrySet()) {
            indegree.put(e.getKey(), e.getValue().size());
            for (String dep : e.getValue()) {
                downstream.get(dep).add(e.getKey());
            }
        }
        Deque<String> queue = new ArrayDeque<>();
        List<String> sorted = new ArrayList<>();
        for (Map.Entry<String, Integer> e : indegree.entrySet()) {
            if (e.getValue() == 0) {
                queue.add(e.getKey());
            }
        }
        while (!queue.isEmpty()) {
            String id = queue.poll();
            sorted.add(id);
            for (String child : downstream.get(id)) {
                int d = indegree.get(child) - 1;
                indegree.put(child, d);
                if (d == 0) {
                    queue.add(child);
                }
            }
        }
        if (sorted.size() != byId.size()) {
            List<String> cyclic = byId.keySet().stream()
                    .filter(id -> !sorted.contains(id))
                    .sorted()
                    .toList();
            throw new WorkflowValidationException(RejectReason.CYCLIC_DEPENDENCY,
                    "cyclic dependency detected involving steps: " + cyclic);
        }
    }

    /**
     * Re-order steps into a topological order (ties broken by step id), and
     * rewrite each step's dependsOn into sorted order. This guarantees the
     * execution graph is identical regardless of the submitted list order.
     */
    private List<StepDef> canonicalize(Map<String, StepDef> byId, Map<String, List<String>> deps) {
        // Recompute a deterministic topological order with an id-sorted ready set.
        Map<String, Integer> indegree = new HashMap<>();
        Map<String, List<String>> downstream = new HashMap<>();
        for (String id : byId.keySet()) {
            indegree.put(id, 0);
            downstream.put(id, new ArrayList<>());
        }
        for (Map.Entry<String, List<String>> e : deps.entrySet()) {
            indegree.put(e.getKey(), e.getValue().size());
            for (String dep : e.getValue()) {
                downstream.get(dep).add(e.getKey());
            }
        }
        java.util.PriorityQueue<String> ready = new java.util.PriorityQueue<>();
        indegree.forEach((id, d) -> {
            if (d == 0) {
                ready.add(id);
            }
        });
        List<String> topo = new ArrayList<>();
        while (!ready.isEmpty()) {
            String id = ready.poll();
            topo.add(id);
            for (String child : downstream.get(id)) {
                int d = indegree.get(child) - 1;
                indegree.put(child, d);
                if (d == 0) {
                    ready.add(child);
                }
            }
        }

        List<StepDef> out = new ArrayList<>();
        for (String id : topo) {
            StepDef s = byId.get(id);
            List<String> sortedDeps = deps.get(id).stream().sorted().toList();
            out.add(new StepDef(s.id(), s.action(), sortedDeps, s.timeoutMillis()));
        }
        return out;
    }
}
