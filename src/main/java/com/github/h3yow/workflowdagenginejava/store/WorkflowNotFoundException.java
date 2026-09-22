package com.github.h3yow.workflowdagenginejava.store;

/** Thrown when no persisted workflow exists for the given id. */
public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(String workflowId) {
        super("workflow not found: " + workflowId);
    }
}
