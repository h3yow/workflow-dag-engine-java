package com.github.h3yow.workflowdagenginejava.model;

import java.util.List;
import java.util.Map;

/**
 * Immutable, persistable snapshot of a workflow instance: definition,
 * aggregated status, per-step traces and failure attribution.
 */
public record WorkflowSnapshot(
        String workflowId,
        WorkflowStatus status,
        int maxConcurrency,
        List<StepDef> steps,
        Map<String, StepRecord> records,
        FailureKind failureKind,
        String failureMessage,
        long createdAt,
        long updatedAt
) {
}
