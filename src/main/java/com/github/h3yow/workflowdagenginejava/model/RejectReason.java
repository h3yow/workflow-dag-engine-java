package com.github.h3yow.workflowdagenginejava.model;

/**
 * Distinguishable reasons for rejecting a workflow submission.
 * The {@code code} is stable and machine-readable.
 */
public enum RejectReason {
    EMPTY_DEFINITION("EMPTY_DEFINITION"),
    EMPTY_WORKFLOW_ID("EMPTY_WORKFLOW_ID"),
    EMPTY_STEP_ID("EMPTY_STEP_ID"),
    DUPLICATE_STEP_ID("DUPLICATE_STEP_ID"),
    SELF_DEPENDENCY("SELF_DEPENDENCY"),
    MISSING_DEPENDENCY("MISSING_DEPENDENCY"),
    CYCLIC_DEPENDENCY("CYCLIC_DEPENDENCY");

    private final String code;

    RejectReason(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
