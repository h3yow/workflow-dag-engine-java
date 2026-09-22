package com.github.h3yow.workflowdagenginejava.validation;

import com.github.h3yow.workflowdagenginejava.model.RejectReason;

/**
 * 工作流定义校验失败时抛出，reason 用于区分拒绝原因。
 */
public class InvalidWorkflowException extends RuntimeException {

    private final RejectReason reason;

    public InvalidWorkflowException(RejectReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectReason getReason() {
        return reason;
    }
}
