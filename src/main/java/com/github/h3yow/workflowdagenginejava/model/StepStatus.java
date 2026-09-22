package com.github.h3yow.workflowdagenginejava.model;

/** Lifecycle status of a single step execution. */
public enum StepStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    TIMEOUT,
    SKIPPED
}
