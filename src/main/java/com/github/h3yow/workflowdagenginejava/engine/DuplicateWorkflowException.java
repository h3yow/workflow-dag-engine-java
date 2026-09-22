package com.github.h3yow.workflowdagenginejava.engine;

/**
 * Thrown when submitting an id that already exists but carries a different
 * definition than the stored one.
 */
public class DuplicateWorkflowException extends RuntimeException {

    public DuplicateWorkflowException(String workflowId) {
        super("workflow id already exists with a different definition: " + workflowId);
    }
}
