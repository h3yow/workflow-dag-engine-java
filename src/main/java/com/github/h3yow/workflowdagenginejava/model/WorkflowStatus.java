package com.github.h3yow.workflowdagenginejava.model;

/** Lifecycle status of a workflow instance. */
public enum WorkflowStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    REJECTED
}
