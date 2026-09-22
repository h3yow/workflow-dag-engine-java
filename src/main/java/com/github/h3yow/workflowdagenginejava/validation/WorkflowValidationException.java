package com.github.h3yow.workflowdagenginejava.validation;

import com.github.h3yow.workflowdagenginejava.model.RejectReason;

/** Thrown when a submitted workflow definition fails dependency validation. */
public class WorkflowValidationException extends RuntimeException {

    private final RejectReason reason;

    public WorkflowValidationException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason reason() {
        return reason;
    }
}
