package com.github.h3yow.workflowdagenginejava.model;

/**
 * Persistent trace record of one step execution. {@code attempts} is used to
 * prove that a succeeded step is never re-executed (it stays at 1 after resume).
 */
public record StepRecord(
        StepStatus status,
        String detail,
        FailureKind failureKind,
        int attempts,
        long startedAt,
        long finishedAt
) {
}
